using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Application.DataLoad.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class CsvDataFileSplitBackgroundService(
    Channel<CsvDataFileSplitJob> channel,
    ILogger<CsvDataFileSplitBackgroundService> logger,
    ISplitJobProgressStore progressStore,
    ICsvDataFileSplitterService csvDataFileSplitterService) : BackgroundService
{
    private readonly Channel<CsvDataFileSplitJob> _channel = channel;
    private readonly ILogger<CsvDataFileSplitBackgroundService> _logger = logger;
    private readonly ISplitJobProgressStore _progressStore = progressStore;
    private readonly int _maxParallelDownloads = 4;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var semaphore = new SemaphoreSlim(_maxParallelDownloads);
        var runningTasks = new ConcurrentBag<Task>();

        await foreach (var request in _channel.Reader.ReadAllAsync(stoppingToken))
        {
            if (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Cancellation requested, aborting split");
                return;
            }

            await semaphore.WaitAsync(stoppingToken);

            var task = Task.Run(
                async () =>
                {
                    try
                    {
                        _progressStore.MarkInProgress(request.JobId, request.Key);

                        var result = await csvDataFileSplitterService.ExecuteAsync(request, stoppingToken);

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
                stoppingToken);

            runningTasks.Add(task);
        }

        await Task.WhenAll(runningTasks);
    }
}