using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.Encryption;
using CadsBridge.Application.Extensions;
using CadsBridge.Core.DataLoad;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using CadsBridge.Infrastructure.DataLoad.Csv.Extensions;
using CadsBridge.Infrastructure.Encryption.Configuration;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Strategies;

public class CsvDataFileSplitterStrategyBySize(
    IS3ClientFactory s3ClientFactory,
    DataLoadConfiguration config,
    IFileEncryptor fileEncryptor,
    IOptions<EncryptionConfig> encryptionOptions,
    ILogger<CsvDataFileSplitterStrategyBySize> logger) :
    ICsvDataFileSplitterStrategy
{
    public SplitType SplitType => SplitType.BySize;

    private readonly EncryptionConfig _encryptionConfig = encryptionOptions.Value;

    public async Task<long> ProcessAsync(CsvDataFileSplitJob job, CancellationToken cancellationToken)
    {
        if (!config.SplitValue.HasValue)
        {
            throw new ArgumentException("Split value must be specified for splitting.");
        }

        var internalS3Info = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        var s3 = internalS3Info.Client;
        var chunkSizeBytes = config.SplitValue * 1024L * 1024L;

        // Get object metadata to know file size
        var metadata = await s3.GetObjectMetadataAsync(internalS3Info.BucketName, job.SourceKey, cancellationToken);
        var totalSize = metadata.ContentLength;

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("Source file size: {SizeMB} MB", totalSize / (1024 * 1024));

        // Get the object from S3
        using var response = await s3.GetObjectAsync(
            new GetObjectRequest { BucketName = internalS3Info.BucketName, Key = job.SourceKey },
            cancellationToken);

        using var reader = new StreamReader(response.ResponseStream, Encoding.UTF8);

        var chunkNumber = 1;
        var bytesInChunk = 0;
        long totalBytesProcessed = 0;
        var chunkStream = new MemoryStream();
        var chunkWriter = new StreamWriter(chunkStream, Encoding.UTF8);

        string? line = null;

        Stream uploadStream;

        while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
        {
            
            if (cancellationToken.IsCancellationRequested)
            {
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation("Cancellation requested for {Key}, aborting split", job.SourceKey);
                return 0;
            }

            var lineBytes = Encoding.UTF8.GetBytes(line + Environment.NewLine);

            uploadStream = chunkStream;

            // If adding this line exceeds chunk size, upload current chunk and start a new one
            if (bytesInChunk + lineBytes.Length > chunkSizeBytes)
            {
                // encrypt the last chunk if encryption is enabled
                if (_encryptionConfig.StorageEncryptionEnabled)
                {
                    uploadStream = await fileEncryptor.EncryptAsync(chunkStream, cancellationToken);
                }

                await s3.UploadChunkAsync(
                    internalS3Info.BucketName,
                    job.SourceKey.FormatSplitFileTargetKey(chunkNumber),
                    uploadStream,
                    cancellationToken: cancellationToken);
                chunkNumber++;
                await chunkStream.DisposeAsync();

                chunkStream = new MemoryStream();
                chunkWriter = new StreamWriter(chunkStream, Encoding.UTF8);
                totalBytesProcessed += bytesInChunk;
                bytesInChunk = 0;
            }

            await chunkWriter.WriteAsync(line + Environment.NewLine);
            await chunkWriter.FlushAsync(cancellationToken);
            bytesInChunk += lineBytes.Length;
        }

        // Upload the last chunk if it has data
        if (bytesInChunk > 0)
        {
            uploadStream = chunkStream;

            // encrypt the last chunk if encryption is enabled
            if (_encryptionConfig.StorageEncryptionEnabled)
            {
                uploadStream = await fileEncryptor.EncryptAsync(chunkStream, cancellationToken);
            }

            totalBytesProcessed += bytesInChunk;
            await s3.UploadChunkAsync(
                internalS3Info.BucketName,
                job.SourceKey.FormatSplitFileTargetKey(chunkNumber),
                uploadStream,
                cancellationToken: cancellationToken);
        }

        return totalBytesProcessed;
    }
}