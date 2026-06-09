using System.Security.Cryptography;
using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.Models;
using CadsBridge.Core.Crypto;
using CadsBridge.Core.Exceptions;
using CadsBridge.Core.Storage.Abstractions;
using CadsBridge.Core.Storage.Clients;
using CadsBridge.Core.Storage.Factories;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Application.Services;

public class FileImportCopyService(
    IS3ClientFactory s3ClientFactory,
    IAesCryptoTransform aesCryptoTransform,
    IAmazonTransferServiceWrapper transferWrapper,
    ILogger<FileImportCopyService> logger) : IFileImportCopyService
{
    private readonly IS3ClientFactory _s3ClientFactory = s3ClientFactory;
    private readonly IAesCryptoTransform _aesCryptoTransform = aesCryptoTransform;

    private readonly int _maxRetries = 3;
    public const long MinPartitionSize = 5L * 1024 * 1024; // 5 MB (S3 minimum)
    private const long MaxSingleFileSize = 100L * 1024 * 1024;

    public async Task<bool> CopyWithRetryAsync(FileImportJob request, CancellationToken cancellationToken = default)
    {
        var attempt = 0;
        var delayBaseMs = 500;

        var externalS3Info = _s3ClientFactory.GetClientInfo<ExternalStorageClient>();
        var externalS3 = externalS3Info.Client;

        var internalS3Info = _s3ClientFactory.GetClientInfo<InternalStorageClient>();
        var internalS3 = internalS3Info.Client;

        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation("Cancellation requested for {Key}, aborting copy", request.SourceKey);
                return false;
            }

            attempt++;

            try
            {
                if (attempt > _maxRetries)
                {
                    throw new RetriesExceededException($"Exceeded maximum retry attempts ({_maxRetries}) for copying {request.SourceKey}");
                }


                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "S3 accelerating copy of {Key} from {SourceBucket} to {DestBucket}, attempt {Attempt}",
                        request.SourceKey,
                        externalS3Info.BucketName,
                        internalS3Info.BucketName,
                        attempt);

                await DecryptAndCopyAsync(request, externalS3Info, internalS3Info, externalS3, internalS3, cancellationToken);

                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "S3 accelerated copy complete: {SourceBucket}/{SourceKey} → {DestBucket}/{DestKey}",
                        externalS3Info.BucketName,
                        request.SourceKey,
                        internalS3Info.BucketName,
                        request.TargetKey);

                break;
            }
            catch (Exception ex) when (attempt < _maxRetries)
            {
                var delay = TimeSpan.FromMilliseconds(delayBaseMs * Math.Pow(2, attempt - 1));

                logger.LogWarning(
                    ex,
                    "Error copying {Key}, attempt {Attempt}/{Max}. Retrying in {Delay}ms",
                    request.SourceKey, attempt, _maxRetries, delay.TotalMilliseconds);

                await Task.Delay(delay, cancellationToken);
            }
        }

        return true;
    }

    private async Task DecryptAndCopyAsync(
        FileImportJob request,
        S3ClientFactory.ClientInfo externalS3Info,
        S3ClientFactory.ClientInfo internalS3Info,
        IAmazonS3 externalS3,
        IAmazonS3 internalS3,
        CancellationToken cancellationToken)
    {

        using var getResponse = await externalS3.GetObjectAsync(externalS3Info.BucketName, request.SourceKey, cancellationToken);
        using var encryptedStream = getResponse.ResponseStream;

        // Determine file size to decide whether to use multipart upload or single upload
        var fileSize = await GetRemoteFileSizeAsync(externalS3, externalS3Info.BucketName, request.SourceKey, cancellationToken);

        // if file is small enough to avoid multipart overhead, otherwise use streaming with multipart upload
        if (fileSize < MaxSingleFileSize)
        {
            using var memoryStream = new MemoryStream();
            memoryStream.Position = 0;
            await _aesCryptoTransform.DecryptStreamAsync(encryptedStream, memoryStream, request.Password, request.Salt, cancellationToken: cancellationToken);
            await PutAsync(internalS3, memoryStream, internalS3Info.BucketName, request.TargetKey, cancellationToken: cancellationToken);
        }
        else
        {
            // Create decryptor
            using var decryptor = AesCryptoTransform.CreateDecryptor(request.Password, request.Salt);
            using var cryptoStream = new CryptoStream(encryptedStream, decryptor, CryptoStreamMode.Read);

            var partitionSize = CalculateOptimalPartSize(fileSize);

            await transferWrapper.TransferAsync(internalS3, cryptoStream, internalS3Info.BucketName, request.TargetKey, partitionSize, cancellationToken: cancellationToken);
        }

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Successfully decrypted and uploaded {Key}", request.TargetKey);
        // Stream encrypted file from S3
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