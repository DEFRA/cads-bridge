using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Application.Storage.Transfer;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Crypto;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Factories;
using Microsoft.Extensions.Logging;
using System.Security.Cryptography;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using CadsBridge.Infrastructure.DataLoad.Csv.Extensions;
using CadsBridge.Infrastructure.DataLoad.Csv.Files;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class S3CopyService(
    IS3ClientFactory s3ClientFactory,
    IAesCryptoTransform aesCryptoTransform,
    ITransferUtilityAdapter transferUtilityAdapter,
    DataLoadConfiguration config,
    ILogger<S3CopyService> logger) : IS3CopyService
{
    private readonly int _maxRetries = 3;
    private const long MinPartitionSize = 5L * 1024 * 1024; // 5 MB (S3 minimum)
    private const long MaxSingleFileSize = 100L * 1024 * 1024;

    public async Task<bool> ExecAsync(CsvDataFileImportJob job, CancellationToken cancellationToken = default)
    {
        var attempt = 0;
        var delayBaseMs = 500;

        var externalS3Info = s3ClientFactory.GetClientInfo<ExternalStorageClient>();
        var externalS3 = externalS3Info.Client;

        var internalS3Info = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        var internalS3 = internalS3Info.Client;

        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.LogInformation("Cancellation requested for {Key}, aborting copy", job.SourceKey);
                }
                return false;
            }

            attempt++;

            try
            {
                if (attempt > _maxRetries)
                {
                    throw new RetriesExceededException($"Exceeded maximum retry attempts ({_maxRetries}) for copying {job.SourceKey}");
                }

                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "S3 accelerating copy of {Key} from {SourceBucket} to {DestBucket}, attempt {Attempt}",
                        job.SourceKey,
                        externalS3Info.BucketName,
                        internalS3Info.BucketName,
                        attempt);

                var targetKey = await DecryptAndCopyAsync(job, externalS3Info, internalS3Info, externalS3, internalS3, cancellationToken);

                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "S3 accelerated copy complete: {SourceBucket}/{SourceKey} → {DestBucket}/{DestKey}",
                        externalS3Info.BucketName,
                        job.SourceKey,
                        internalS3Info.BucketName,
                        targetKey);

                break;
            }
            catch (Exception ex) when (attempt < _maxRetries)
            {
                var delay = TimeSpan.FromMilliseconds(delayBaseMs * Math.Pow(2, attempt - 1));

                logger.LogWarning(
                    ex,
                    "Error copying {Key}, attempt {Attempt}/{Max}. Retrying in {Delay}ms",
                    job.SourceKey, attempt, _maxRetries, delay.TotalMilliseconds);

                await Task.Delay(delay, cancellationToken);
            }
        }

        return true;
    }

    private async Task<string> DecryptAndCopyAsync(
        CsvDataFileImportJob request,
        S3ClientFactory.ClientInfo externalS3Info,
        S3ClientFactory.ClientInfo internalS3Info,
        IAmazonS3 externalS3,
        IAmazonS3 internalS3,
        CancellationToken cancellationToken)
    {
        using var getResponse = await externalS3.GetObjectAsync(externalS3Info.BucketName, request.SourceKey, cancellationToken);
        await using var encryptedStream = getResponse.ResponseStream;

        var targetKey = request.TargetKey;

        // Determine file size to decide whether to use multipart upload or single upload
        var fileSize = await GetRemoteFileSizeAsync(externalS3, externalS3Info.BucketName, request.SourceKey, cancellationToken);
        var password = CtsmFilenameParser.Parse(Path.GetFileName(request.SourceKey))!.DerivePassword();

        // if file is small enough to avoid multipart overhead, otherwise use streaming with multipart upload
        if (fileSize < MaxSingleFileSize)
        {
            using var memoryStream = new MemoryStream();
            memoryStream.Position = 0;
            await aesCryptoTransform.DecryptStreamAsync(encryptedStream, memoryStream, password, config.Salt, cancellationToken: cancellationToken);
            await PutAsync(internalS3, memoryStream, internalS3Info.BucketName, targetKey, cancellationToken: cancellationToken);
        }
        else
        {
            // Create decryptor
            using var decryptor = AesCryptoTransform.CreateDecryptor(password, config.Salt);
            using var cryptoStream = new CryptoStream(encryptedStream, decryptor, CryptoStreamMode.Read);

            var partitionSize = CalculateOptimalPartSize(fileSize);

            var transferUtilityUploadRequest = new TransferUtilityUploadRequest
            {
                InputStream = cryptoStream,
                BucketName = internalS3Info.BucketName,
                Key = targetKey,
                StorageClass = S3StorageClass.Standard,
                PartSize = partitionSize, // 5 MB minimum for multipart
                AutoCloseStream = true,
                ContentType = "text/plain"
            };

            await transferUtilityAdapter.UploadAsync(internalS3, transferUtilityUploadRequest, cancellationToken);
        }

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("Successfully decrypted and uploaded {Key}", targetKey);
        }

        return targetKey;
    }

    private static long CalculateOptimalPartSize(long fileSizeBytes)
    {
        // AWS recommendation:
        // For files< 100 MB: Single PUT(no multipart needed).
        // For files 100 MB – 5 GB: Multipart with 8–64 MB parts.
        // For files > 5 GB: Larger part sizes(e.g., 64–128 MB) to reduce part count.

        if (fileSizeBytes <= 0)
            throw new ArgumentException("File size must be greater than zero.", nameof(fileSizeBytes));

        const long RecommendedMin = 8L * 1024 * 1024; // 8 MB (better performance)
        const long RecommendedMax = 128L * 1024 * 1024; // 128 MB (avoid huge retries)
        const int MaxParts = 10_000;

        // Calculate minimum size to not exceed 10,000 parts
        var requiredPartSize = (long)Math.Ceiling((double)fileSizeBytes / MaxParts);

        // Ensure part size is at least the S3 minimum
        var optimalPartSize = Math.Max(MinPartitionSize, requiredPartSize);

        // Apply recommended lower bound for performance
        if (optimalPartSize < RecommendedMin)
            optimalPartSize = RecommendedMin;

        // Cap at recommended max unless file is extremely large
        if (optimalPartSize > RecommendedMax && fileSizeBytes < (RecommendedMax * MaxParts))
            optimalPartSize = RecommendedMax;

        return optimalPartSize;
    }

    private static async Task<long> GetRemoteFileSizeAsync(IAmazonS3 s3Client, string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var metadata = await s3Client.GetObjectMetadataAsync(bucketName, key, cancellationToken);
        return metadata.ContentLength;
    }

    private static async Task PutAsync(
        IAmazonS3 s3,
        Stream stream,
        string bucketName,
        string key,
        string contentType = "text/plain",
        CancellationToken cancellationToken = default)
    {
        if (stream == null || stream.Length == 0)
            throw new ArgumentException("Stream is null or empty.");

        var request = new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            InputStream = stream,
            ContentType = contentType // Adjust MIME type as needed
        };

        await s3.PutObjectAsync(request, cancellationToken);
    }
}