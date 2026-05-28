using CadsBridge.Application.Models;
using CadsBridge.Application.Persistance;
using CadsBridge.Core.Storage.Abstractions;
using CadsBridge.Core.Storage.Clients;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace CadsBridge.Application.Services;

public class FileSplitBackgroundService(
    Channel<FileSplitJob> channel,
    ILogger<FileSplitBackgroundService> logger,
    ISplitJobProgressStore progressStore,
    IS3ClientFactory s3ClientFactory,
    IFileSplitter splitter) : BackgroundService
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

            var task = Task.Run(
                async () =>
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
                        _logger.LogError(ex, "Failed to split file {Key}", request.Key);
                        _progressStore.MarkFailed(request.JobId, request.Key, ex.Message);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                },
                cancellationToken);

            runningTasks.Add(task);
        }

        await Task.WhenAll(runningTasks);
    }

    private async Task<bool> SplitAsync(
        FileSplitJob request,
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
                    request.Key,
                    internalS3Info.BucketName,
                    attempt);

                if (!request.SplitValue.HasValue)
                {
                    throw new ArgumentException("Split value must be specified for splitting.");
                }

                switch (request.SplitType)
                {
                    case SplitType.ByLines:
                        await splitter.SplitFileByLineAsync(
                            internalS3,
                            internalS3Info.BucketName,
                            request.Key,
                            request.TargetFolder,
                            request.SplitValue.Value,
                            cancellationToken);
                        break;

                    case SplitType.BySize:
                        await splitter.SplitFileBySizeAsync(
                            internalS3,
                            internalS3Info.BucketName,
                            request.Key,
                            request.TargetFolder,
                            request.SplitValue.Value,
                            cancellationToken);
                        break;

                    default:
                        throw new ArgumentException("Invalid SplitType specified");
                }

                _logger.LogInformation(
                    "S3 file split complete: {SourceBucket}/{SourceKey}",
                    internalS3Info.BucketName,
                    request.Key);

                break;
            }
            catch (Exception ex) when (attempt < _maxRetries)
            {
                var delay = TimeSpan.FromMilliseconds(delayBaseMs * Math.Pow(2, attempt - 1));

                _logger.LogWarning(
                    ex,
                    "Error splitting {Key}, attempt {Attempt}/{Max}. Retrying in {Delay}ms",
                    request.Key,
                    attempt,
                    _maxRetries,
                    delay.TotalMilliseconds);

                await Task.Delay(delay, cancellationToken);
            }
        }

        return true;
    }

}