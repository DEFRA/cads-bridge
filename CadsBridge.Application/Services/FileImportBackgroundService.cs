using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using CadsBridge.Application.Models;
using CadsBridge.Application.Persistance;
using CadsBridge.Infrastructure.Crypto;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Threading.Channels;

namespace CadsBridge.Application.Services;

public class FileImportBackgroundService(
    Channel<FileImportJob> channel,
    ILogger<FileImportBackgroundService> logger,
    IImportProgressStore progressStore,
    IS3ClientFactory s3ClientFactory,
    IAesCryptoTransform aesCryptoTransform) : BackgroundService
{
    private readonly Channel<FileImportJob> _channel = channel;
    private readonly ILogger<FileImportBackgroundService> _logger = logger;
    private readonly IImportProgressStore _progressStore = progressStore;
    private readonly IS3ClientFactory _s3ClientFactory = s3ClientFactory;
    private readonly IAesCryptoTransform _aesCryptoTransform = aesCryptoTransform;
    private readonly int _maxParallelDownloads = 4;
    private readonly int _maxRetries = 3;

    protected override async Task ExecuteAsync(CancellationToken cancellationToken = default)
    {
        var semaphore = new SemaphoreSlim(_maxParallelDownloads);
        var runningTasks = new ConcurrentBag<Task>();

        await foreach (var request in _channel.Reader.ReadAllAsync(cancellationToken))
        {
            await semaphore.WaitAsync(cancellationToken);

            var task = Task.Run(async () =>
            {
                try
                {
                    _progressStore.MarkInProgress(request.JobId, request.SourceKey);
                    await CopyWithRetryAsync(request, cancellationToken);
                    _progressStore.MarkSucceeded(request.JobId, request.SourceKey);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to import {Key}", request.SourceKey);
                    _progressStore.MarkFailed(request.JobId, request.SourceKey, ex.Message);
                }
                finally
                {
                    semaphore.Release();
                }
            }, cancellationToken);

            runningTasks.Add(task);
        }

        await Task.WhenAll(runningTasks);
    }

    private async Task CopyWithRetryAsync(FileImportJob request, CancellationToken ct)
    {
        var attempt = 0;
        var delayBaseMs = 500;

        var externalS3 = _s3ClientFactory.GetClient<ExternalClient>();
        var externalS3Info = _s3ClientFactory.GetClientInfo<ExternalClient>();

        var internalS3 = _s3ClientFactory.GetClient<InternalClient>();
        var internalS3Info = _s3ClientFactory.GetClientInfo<InternalClient>();

        while (true)
        {
            attempt++;

            try
            {
                _logger.LogInformation(
                    "S3 accelerating copy of {Key} from {SourceBucket} to {DestBucket}, attempt {Attempt}",
                    request.SourceKey, externalS3Info.BucketName, internalS3Info.BucketName, attempt);

                await DecryptAndCopyAsync(
                    externalS3,
                    internalS3,
                    externalS3Info.BucketName,
                    request.SourceKey,
                    internalS3Info.BucketName,
                    request.TargetKey,
                    request.Password,
                    request.Salt);

                _logger.LogInformation(
                    "S3 accelerated copy complete: {SourceBucket}/{SourceKey} → {DestBucket}/{DestKey}",
                    externalS3Info.BucketName, request.SourceKey,
                    internalS3Info.BucketName, request.TargetKey);
            }
            catch (Exception ex) when (attempt < _maxRetries)
            {
                var delay = TimeSpan.FromMilliseconds(delayBaseMs * Math.Pow(2, attempt - 1));

                _logger.LogWarning(
                    ex,
                    "Error copying {Key}, attempt {Attempt}/{Max}. Retrying in {Delay}ms",
                    request.SourceKey, attempt, _maxRetries, delay.TotalMilliseconds);

                await Task.Delay(delay, ct);
            }
        }
    }

    private async Task DecryptAndCopyAsync(
       IAmazonS3 sourceS3Client,
       IAmazonS3 targetS3Client,
       string sourceBucket,
       string sourceKey,
       string destinationBucket,
       string destinationKey,
       string password,
       string salt,
       CancellationToken ct = default)
    {
        try
        {
            // 2. Stream encrypted file from S3
            using var getResponse = await sourceS3Client.GetObjectAsync(sourceBucket, sourceKey, ct);
            using var encryptedStream = getResponse.ResponseStream;

            // 3. Determine file size to decide whether to use multipart upload or single upload
            var fileSize = await GetRemoteFileSizeAsync(sourceS3Client, sourceBucket, sourceKey);

            if (fileSize < 5 * 1024 * 1024)
            {
                using var memoryStream = new MemoryStream();
                memoryStream.Position = 0;
                await _aesCryptoTransform.DecryptStreamAsync(encryptedStream, memoryStream, password, salt);
                await PutAsync(targetS3Client, memoryStream, destinationBucket, destinationKey);
            }
            else
            {
                // 4. Create decryptor
                using var decryptor = AesCryptoTransform.CreateDecryptor(password, salt);
                using var cryptoStream = new CryptoStream(encryptedStream, decryptor, CryptoStreamMode.Read);

                await TransferAsync(targetS3Client, cryptoStream, destinationBucket, destinationKey);
            }

            _logger.LogInformation("Successfully decrypted and uploaded {Key}", destinationKey);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error decrypting file");

            throw;
        }
    }

    private static async Task<long> GetRemoteFileSizeAsync(IAmazonS3 s3Client, string bucketName, string key)
    {
        try
        {
            var metadata = await s3Client.GetObjectMetadataAsync(bucketName, key);
            return metadata.ContentLength;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // File doesn't exist in S3
            return 0;
        }
    }

    private static async Task PutAsync(IAmazonS3 s3Client, Stream stream, string bucketName, string key)
    {
        if (stream == null || stream.Length == 0)
            throw new ArgumentException("Stream is null or empty.");

        // Reset stream position to ensure full upload
        //if (stream.CanSeek)
        //    stream.Position = 0;

        var request = new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            InputStream = stream,
            ContentType = "text/plain" // Adjust MIME type as needed
        };

        await s3Client.PutObjectAsync(request);
    }

    private static async Task TransferAsync(IAmazonS3 s3Client, Stream stream, string bucketName, string key)
    {
        var transferUtility = new TransferUtility(s3Client);

        var uploadRequest = new TransferUtilityUploadRequest
        {
            InputStream = stream,
            BucketName = bucketName,
            Key = key,
            StorageClass = S3StorageClass.Standard,
            PartSize = 5 * 1024 * 1024, // 5 MB minimum for multipart
            AutoCloseStream = true
        };

        await transferUtility.UploadAsync(uploadRequest);
    }
}