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
    IFileImportStatusStore fileImportStatusStore,
    ICsvDataFileSplitterService csvDataFileSplitterService) : BackgroundService
{
    private readonly int _maxParallelDownloads = 4;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var semaphore = new SemaphoreSlim(_maxParallelDownloads);
        var runningTasks = new ConcurrentBag<Task>();

        await foreach (var request in channel.Reader.ReadAllAsync(stoppingToken))
        {
            if (stoppingToken.IsCancellationRequested)
            {
                logger.LogInformation("Cancellation requested, aborting split");
                return;
            }

            if (!request.FileImportStatusId.HasValue)
            {
                logger.LogError("FileImportStatusId is required for split job {Key}", request.Key);
                progressStore.MarkFailed(request.JobId, request.Key, "FileImportStatusId is required for split job");
                continue;
            }

            await semaphore.WaitAsync(stoppingToken);

            var task = Task.Run(
                async () =>
                {
                    try
                    {
                        progressStore.MarkInProgress(request.JobId, request.Key);
                        var result = await csvDataFileSplitterService.ExecuteAsync(request, stoppingToken);

                        if (result)
                        {
                            progressStore.MarkSucceeded(request.JobId, request.Key);
                            await fileImportStatusStore.MarkSucceeded(request.FileImportStatusId.Value, stoppingToken);
                        }
                        else
                        {
                            progressStore.MarkFailed(request.JobId, request.Key, "Unknown error during split");
                            await fileImportStatusStore.MarkFailed(request.FileImportStatusId.Value, stoppingToken);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to split file {Key}", request.Key);
                        progressStore.MarkFailed(request.JobId, request.Key, ex.Message);
                        await fileImportStatusStore.MarkFailed(request.FileImportStatusId.Value, stoppingToken);
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