using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.Models;
using CadsBridge.Application.Persistance;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Channels;

namespace CadsBridge.Application.Services;

public class FileSplitBackgroundService(
    Channel<FileSplitJob> channel,
    ILogger<FileSplitBackgroundService> logger,
    ISplitJobProgressStore progressStore,
    IS3ClientFactory s3ClientFactory) : BackgroundService
{
    private readonly Channel<FileSplitJob> _channel = channel;
    private readonly ILogger<FileSplitBackgroundService> _logger = logger;
    private readonly ISplitJobProgressStore _progressStore = progressStore;
    private readonly IS3ClientFactory _s3ClientFactory = s3ClientFactory;
    private readonly int _maxParallelDownloads = 4;
    private readonly int _maxRetries = 3;

    protected override async Task ExecuteAsync(CancellationToken cancellationToken = default)
    {
        var semaphore = new SemaphoreSlim(_maxParallelDownloads);
        var runningTasks = new ConcurrentBag<Task>();

        await foreach (var request in _channel.Reader.ReadAllAsync(cancellationToken))
        {
            if (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("Cancellation requested, aborting split");
                return;
            }

            await semaphore.WaitAsync(cancellationToken);

            var task = Task.Run(async () =>
            {
                try
                {
                    _progressStore.MarkInProgress(request.JobId, request.Key);

                    var result = await SplitAsync(request, cancellationToken);

                    if (result)
                    {
                        _progressStore.MarkSucceeded(request.JobId, request.Key);
                    }
                    else
                    {
                        _progressStore.MarkFailed(request.JobId, request.Key, "Unknown error during splt");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to splt file {Key}", request.Key);
                    _progressStore.MarkFailed(request.JobId, request.Key, ex.Message);
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

    private async Task<bool> SplitAsync(FileSplitJob request, 
        CancellationToken cancellationToken = default)
    {
        var attempt = 0;
        var delayBaseMs = 500;

        var internalS3Info = _s3ClientFactory.GetClientInfo<InternalStorageClient>();
        var internalS3 = internalS3Info.Client;

        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("Cancellation requested for {Key}, aborting split", request.Key);
                return false;
            }

            attempt++;

            try
            {
                if (attempt > _maxRetries)
                {
                    throw new Exception($"Exceeded maximum retry attempts ({_maxRetries}) for splitting {request.Key}");
                }

                _logger.LogInformation(
                    "S3 splitting copy of {Key} from {SourceBucket}, attempt {Attempt}",
                    request.Key, internalS3Info.BucketName, attempt);

                if (!request.SplitValue.HasValue)
                {
                    throw new ArgumentException("Split value must be specified for splitting.");
                }

                switch (request.SplitType)
                {
                    case SplitType.ByLines:
                        await SplitFileByLineAsync(internalS3, internalS3Info.BucketName, request.Key, request.TargetFolder, request.SplitValue.Value, cancellationToken);
                        break;

                    case SplitType.BySize:
                        await SplitFileBySizeAsync(internalS3, internalS3Info.BucketName, request.Key, request.TargetFolder, request.SplitValue.Value, cancellationToken);
                        break;

                    default:
                        throw new ArgumentException("Invalid SplitType specified");
                }
            
                _logger.LogInformation(
                    "S3 file split complete: {SourceBucket}/{SourceKey}",
                    internalS3Info.BucketName, request.Key);

                break;
            }
            catch (Exception ex) when (attempt < _maxRetries)
            {
                var delay = TimeSpan.FromMilliseconds(delayBaseMs * Math.Pow(2, attempt - 1));

                _logger.LogWarning(
                    ex,
                    "Error splitting {Key}, attempt {Attempt}/{Max}. Retrying in {Delay}ms",
                    request.Key, attempt, _maxRetries, delay.TotalMilliseconds);

                await Task.Delay(delay, cancellationToken);
            }
        }

        return true;
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

        _logger.LogInformation("Source file size: {SizeMB} MB", totalSize / (1024 * 1024));

        // Get the object from S3
        using var response = await s3.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = sourceKey
        }, cancellationToken);

        using var reader = new StreamReader(response.ResponseStream, Encoding.UTF8);

        var chunkNumber = 1;
        var bytesInChunk = 0;
        var chunkStream = new MemoryStream();
        var chunkWriter = new StreamWriter(chunkStream, Encoding.UTF8);

        string line;

        while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("Cancellation requested for {Key}, aborting split", sourceKey);
                return;
            }

            var lineBytes = Encoding.UTF8.GetBytes(line + Environment.NewLine);

            // If adding this line exceeds chunk size, upload current chunk and start a new one
            if (bytesInChunk + lineBytes.Length > chunkSizeBytes)
            {
                await UploadChunkAsync(s3, bucketName, destinationPrefix, sourceKey, chunkNumber++, chunkStream, cancellationToken: cancellationToken);
                chunkStream.Dispose();

                chunkStream = new MemoryStream();
                chunkWriter = new StreamWriter(chunkStream, Encoding.UTF8);
                bytesInChunk = 0;
            }

            await chunkWriter.WriteAsync(line + Environment.NewLine);
            await chunkWriter.FlushAsync();
            bytesInChunk += lineBytes.Length;
        }

        // Upload the last chunk if it has data
        if (bytesInChunk > 0)
        {
            await UploadChunkAsync(s3, bucketName, destinationPrefix, sourceKey, chunkNumber, chunkStream, cancellationToken: cancellationToken);
        }
    }

    public async Task SplitFileByLineAsync(
        IAmazonS3 s3,
        string bucketName,
        string sourceKey,
        string destinationPrefix,
        int linesPerChunk,
        CancellationToken cancellationToken = default)
    {
        using var response = await s3.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = sourceKey
        }, cancellationToken);

        using var reader = new StreamReader(response.ResponseStream);

        // read the file header information, should be the first line in the file.
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

        var chunkNumber = 1;
        var lineCount = 0;
        var chunkBuilder = new StringBuilder();
        chunkBuilder.AppendLine(header);
        chunkBuilder.AppendLine(columns);

        while (await reader.ReadLineAsync(cancellationToken) is { } line)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("Cancellation requested for {Key}, aborting split", sourceKey);
                return;
            }

            chunkBuilder.AppendLine(line);
            lineCount++;

            if (lineCount >= linesPerChunk)
            {
                await UploadChunkAsync(
                    s3,
                    bucketName,
                    destinationPrefix,
                    sourceKey,
                    chunkNumber,
                    chunkBuilder.ToString(),
                    cancellationToken: cancellationToken);

                chunkNumber++;
                lineCount = 0;
                chunkBuilder.Clear();
                chunkBuilder.AppendLine(header);
                chunkBuilder.AppendLine(columns);
            }
        }

        if (lineCount > 0)
        {
            await UploadChunkAsync(
                s3,
                bucketName,
                destinationPrefix,
                sourceKey,
                chunkNumber,
                chunkBuilder.ToString(),
                cancellationToken: cancellationToken);
        }

        return;
    }

    private static async Task<string> UploadChunkAsync(
        IAmazonS3 s3,
        string bucketName,
        string destinationPrefix,
        string sourceKey,
        int chunkNumber,
        string content,
        string contentType = "text/csv",
        CancellationToken cancellationToken = default)
    {
        await using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(content));

        return await UploadChunkAsync(
            s3, 
            bucketName, 
            destinationPrefix, 
            sourceKey, 
            chunkNumber, 
            inputStream, 
            contentType, 
            cancellationToken);
    }

    private static async Task<string> UploadChunkAsync(
        IAmazonS3 s3,
        string bucketName,
        string destinationPrefix,
        string sourceKey,
        int chunkNumber,
        MemoryStream inputStream, 
        string contentType = "text/csv",
        CancellationToken cancellationToken = default)
    {
        var fileName = $"{Path.GetFileNameWithoutExtension(sourceKey)}.part-{chunkNumber:D4}.csv";
        var key = fileName;

        if (!string.IsNullOrEmpty(destinationPrefix))
        {
            key = $"{destinationPrefix.TrimEnd('/')}/{fileName}";
        }

        inputStream.Position = 0; // Reset stream position before upload

        await s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            InputStream = inputStream,
            ContentType = contentType
        }, cancellationToken);

        return key;
    }
}