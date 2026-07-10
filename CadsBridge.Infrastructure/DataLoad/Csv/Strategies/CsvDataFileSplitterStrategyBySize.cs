using System.Text;
using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.DataLoad.Csv.Extensions;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Strategies;

public class CsvDataFileSplitterStrategyBySize(
    IS3ClientFactory s3ClientFactory,
    ILogger<CsvDataFileSplitterStrategyBySize> logger) :
    ICsvDataFileSplitterStrategy
{
    public SplitType SplitType => SplitType.BySize;

    public async Task Process(CsvDataFileSplitJob job, CancellationToken cancellationToken)
    {
        if (!job.SplitValue.HasValue)
        {
            throw new ArgumentException("Split value must be specified for splitting.");
        }

        var internalS3Info = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        var s3 = internalS3Info.Client;
        var chunkSizeBytes = job.SplitValue * 1024L * 1024L;

        // Get object metadata to know file size
        var metadata = await s3.GetObjectMetadataAsync(internalS3Info.BucketName, job.Key, cancellationToken);
        var totalSize = metadata.ContentLength;

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Source file size: {SizeMB} MB", totalSize / (1024 * 1024));

        // Get the object from S3
        using var response = await s3.GetObjectAsync(
            new GetObjectRequest { BucketName = internalS3Info.BucketName, Key = job.Key },
            cancellationToken);

        using var reader = new StreamReader(response.ResponseStream, Encoding.UTF8);

        var chunkNumber = 1;
        var bytesInChunk = 0;
        var chunkStream = new MemoryStream();
        var chunkWriter = new StreamWriter(chunkStream, Encoding.UTF8);

        string? line = null;

        while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation("Cancellation requested for {Key}, aborting split", job.Key);
                return;
            }

            var lineBytes = Encoding.UTF8.GetBytes(line + Environment.NewLine);

            // If adding this line exceeds chunk size, upload current chunk and start a new one
            if (bytesInChunk + lineBytes.Length > chunkSizeBytes)
            {
                var chunkNumber1 = chunkNumber++;
                await s3.UploadChunkAsync(
                    internalS3Info.BucketName,
                    job.Key.FormatKey(job.TargetFolder, chunkNumber1),
                    chunkStream,
                    cancellationToken: cancellationToken);
                await chunkStream.DisposeAsync();

                chunkStream = new MemoryStream();
                chunkWriter = new StreamWriter(chunkStream, Encoding.UTF8);
                bytesInChunk = 0;
            }

            await chunkWriter.WriteAsync(line + Environment.NewLine);
            await chunkWriter.FlushAsync(cancellationToken);
            bytesInChunk += lineBytes.Length;
        }

        // Upload the last chunk if it has data
        if (bytesInChunk > 0)
        {
            await s3.UploadChunkAsync(
                internalS3Info.BucketName,
                job.Key.FormatKey(job.TargetFolder, chunkNumber),
                chunkStream,
                cancellationToken: cancellationToken);
        }
    }
}