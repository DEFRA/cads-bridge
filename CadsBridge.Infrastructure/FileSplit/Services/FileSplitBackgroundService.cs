using System.Collections.Concurrent;
using System.Threading.Channels;
using CadsBridge.Application.FileSplit.Services;
using CadsBridge.Application.Models;
using CadsBridge.Application.Persistence;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.FileSplit.Services;

public class FileSplitBackgroundService(
    Channel<FileSplitJob> channel,
    ILogger<FileSplitBackgroundService> logger,
    ISplitJobProgressStore progressStore,
    IS3FileSplitterService s3FileSplitter) : BackgroundService
{
    private readonly Channel<FileSplitJob> _channel = channel;
    private readonly ILogger<FileSplitBackgroundService> _logger = logger;
    private readonly ISplitJobProgressStore _progressStore = progressStore;
    private readonly int _maxParallelDownloads = 4;

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
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

                        var result = await s3FileSplitter.ExecuteAsync(request, cancellationToken);

                        if (result)
                        {
                            _progressStore.MarkSucceeded(request.JobId, request.Key);
                        }
                        else
                        {
                            _progressStore.MarkFailed(request.JobId, request.Key, "Unknown error during split");
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
}