using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.Models;
using CadsBridge.Core.Exceptions;
using CadsBridge.Core.Storage.Abstractions;
using CadsBridge.Core.Storage.Clients;
using CadsBridge.Core.Storage.Factories;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Application.Services;

public class S3FileSplitterService(
    IS3ClientFactory s3ClientFactory,
    ILogger<S3FileSplitterService> logger)
    : IS3FileSplitterService
{
    private readonly int _maxRetries = 3;
    private readonly int _delayBaseMs = 500;

    public async Task<bool> ExecuteAsync(FileSplitJob request, CancellationToken cancellationToken)
    {
        if (!request.SplitValue.HasValue)
        {
            throw new ArgumentException("Split value must be specified for splitting.");
        }

        var attempt = 0;
        var internalS3Info = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation("Cancellation requested for {Key}, aborting split", request.Key);
                return false;
            }

            attempt++;
            if (attempt > _maxRetries)
            {
                throw new RetriesExceededException($"Exceeded maximum retry attempts ({_maxRetries}) for splitting {request.Key}");
            }

            try
            {
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "S3 splitting copy of {Key} from {SourceBucket}, attempt {Attempt}",
                        request.Key,
                        internalS3Info.BucketName,
                        attempt);

                await SplitFileAsync(request, internalS3Info, cancellationToken);

                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "S3 file split complete: {SourceBucket}/{SourceKey}",
                        internalS3Info.BucketName,
                        request.Key);

                return true;
            }
            catch (Exception ex) when (attempt < _maxRetries)
            {
                var delay = TimeSpan.FromMilliseconds(_delayBaseMs * Math.Pow(2, attempt - 1));

                logger.LogWarning(
                    ex,
                    "Error splitting {Key}, attempt {Attempt}/{Max}. Retrying in {Delay}ms",
                    request.Key,
                    attempt,
                    _maxRetries,
                    delay.TotalMilliseconds);

                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    private async Task SplitFileAsync(
        FileSplitJob request,
        S3ClientFactory.ClientInfo internalS3Info,
        CancellationToken cancellationToken)
    {
        switch (request.SplitType)
        {
            case SplitType.ByLines:
                await SplitFileByLineAsync(
                    internalS3Info.Client,
                    internalS3Info.BucketName,
                    request.Key,
                    request.TargetFolder,
                    request.SplitValue!.Value,
                    cancellationToken);
                break;

            case SplitType.BySize:
                await SplitFileBySizeAsync(
                    internalS3Info.Client,
                    internalS3Info.BucketName,
                    request.Key,
                    request.TargetFolder,
                    request.SplitValue!.Value,
                    cancellationToken);
                break;

            default:
                throw new ArgumentException("Invalid SplitType specified");
        }
    }

    private async Task SplitFileBySizeAsync(
        IAmazonS3 s3,
        string bucketName,
        string sourceKey,
        string destinationPrefix,
        int chunkSizeMB,
        CancellationToken cancellationToken = default)
    {
        var chunkSizeBytes = chunkSizeMB * 1024L * 1024L;

        // Get object metadata to know file size
        var metadata = await s3.GetObjectMetadataAsync(bucketName, sourceKey, cancellationToken);
        var totalSize = metadata.ContentLength;

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Source file size: {SizeMB} MB", totalSize / (1024 * 1024));

        // Get the object from S3
        using var response = await s3.GetObjectAsync(
            new GetObjectRequest { BucketName = bucketName, Key = sourceKey },
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
                    logger.LogInformation("Cancellation requested for {Key}, aborting split", sourceKey);
                return;
            }

            var lineBytes = Encoding.UTF8.GetBytes(line + Environment.NewLine);

            // If adding this line exceeds chunk size, upload current chunk and start a new one
            if (bytesInChunk + lineBytes.Length > chunkSizeBytes)
            {
                int chunkNumber1 = chunkNumber++;
                string fileName = $"{Path.GetFileNameWithoutExtension(sourceKey)}.part-{chunkNumber1:D4}.csv";
                await UploadChunkAsync(
                    s3,
                    bucketName,
                    chunkStream,
                    string.IsNullOrEmpty(destinationPrefix) ? fileName : $"{destinationPrefix.TrimEnd('/')}/{fileName}",
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
            string fileName = $"{Path.GetFileNameWithoutExtension(sourceKey)}.part-{chunkNumber:D4}.csv";
            await UploadChunkAsync(
                s3,
                bucketName,
                chunkStream,
                string.IsNullOrEmpty(destinationPrefix) ? fileName : $"{destinationPrefix.TrimEnd('/')}/{fileName}",
                cancellationToken: cancellationToken);
        }
    }

    private async Task SplitFileByLineAsync(
        IAmazonS3 s3,
        string bucketName,
        string sourceKey,
        string destinationPrefix,
        int linesPerChunk,
        CancellationToken cancellationToken = default)
    {
        using var response = await s3.GetObjectAsync(
            new GetObjectRequest { BucketName = bucketName, Key = sourceKey },
            cancellationToken);

        using var reader = new StreamReader(response.ResponseStream);

        // read the file header information, should be the first line in the file.
        // IGNORED: We are not using the header information in the splitting process,
        // but we read it to ensure we start processing from the correct line and
        // to maintain the structure of the CSV in the output chunks.
        var header = await reader.ReadLineAsync(cancellationToken);
        if (header is null)
        {
            return;
        }

        // read the column definitions, should be the second line in the file.
        var columns = await reader.ReadLineAsync(cancellationToken);
        if (columns is null)
        {
            return;
        }

        // Process the column definitions to remove the first column
        // and apply lowercase to the remaining columns.
        columns = ProcessColumnDefinitions(columns, '|');

        var chunkNumber = 1;
        var lineCount = 0;
        var chunkBuilder = new StringBuilder();

        chunkBuilder.AppendLine(columns);

        while (await reader.ReadLineAsync(cancellationToken) is { } line)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation("Cancellation requested for {Key}, aborting split", sourceKey);
                return;
            }

            chunkBuilder.AppendLine(line);
            lineCount++;

            if (lineCount >= linesPerChunk)
            {
                await UploadChunkAsync(
                    s3,
                    bucketName,
                    chunkBuilder.ToString(),
                    FormatKeyFromFilename(destinationPrefix, sourceKey, chunkNumber),
                    cancellationToken: cancellationToken);

                chunkNumber++;
                lineCount = 0;
                chunkBuilder.Clear();

                chunkBuilder.AppendLine(columns);
            }
        }

        if (lineCount > 0)
        {
            await UploadChunkAsync(
                s3,
                bucketName,
                chunkBuilder.ToString(),
                FormatKeyFromFilename(destinationPrefix, sourceKey, chunkNumber),
                cancellationToken: cancellationToken);
        }
    }

    private static string ProcessColumnDefinitions(string columns, char delimiter)
    {
        // Apply lowercase to each column name for consistency with downstream processing expectations
        columns = columns.ToLower();

        var columnList = columns.Split(delimiter).ToList();

        // Remove the first column which is assumed to be a redundant 'C' column
        columnList.RemoveAt(0);

        return string.Join(delimiter, columnList);
    }

    private static async Task UploadChunkAsync(
        IAmazonS3 s3,
        string bucketName,
        string content,
        string key,
        string contentType = "text/csv",
        CancellationToken cancellationToken = default)
    {
        await using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(content));

        await UploadChunkAsync(
            s3,
            bucketName,
            inputStream,
            key,
            contentType,
            cancellationToken);
    }

    private static async Task UploadChunkAsync(
        IAmazonS3 s3,
        string bucketName,
        MemoryStream inputStream,
        string key,
        string contentType = "text/csv",
        CancellationToken cancellationToken = default)
    {
        inputStream.Position = 0; // Reset stream position before upload

        await s3.PutObjectAsync(
            new PutObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                InputStream = inputStream,
                ContentType = contentType
            },
            cancellationToken);
    }

    private static string FormatKeyFromFilename(string destinationPrefix, string sourceKey, int chunkNumber)
    {
        string fileName = $"{Path.GetFileNameWithoutExtension(sourceKey)}.part-{chunkNumber:D4}.csv";
        return string.IsNullOrEmpty(destinationPrefix) ? fileName : $"{destinationPrefix.TrimEnd('/')}/{fileName}";
    }
}