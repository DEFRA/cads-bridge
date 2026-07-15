using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class CsvDataFileSplitBackgroundService(
    Channel<CsvDataFileSplitJob> channel,
    ILogger<CsvDataFileSplitBackgroundService> logger,
    IFileImportStore fileImportStore,
    ICsvDataFileSplitterService csvDataFileSplitterService,
    DataLoadConfiguration config) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var semaphore = new SemaphoreSlim(config.MaxParallelDownloads);
        var runningTasks = new ConcurrentBag<Task>();

        await foreach (var request in channel.Reader.ReadAllAsync(stoppingToken))
        {
            if (stoppingToken.IsCancellationRequested)
            {
                logger.LogInformation("Cancellation requested, aborting split");
                return;
            }

            if (!request.FileImportId.HasValue)
            {
                logger.LogError("FileImportId is required for split job {Key}", request.SourceKey);
                continue;
            }

            await semaphore.WaitAsync(stoppingToken);

            var task = Task.Run(
                async () =>
                {
                    try
                    {
                        var foundRows = await csvDataFileSplitterService.ExecuteAsync(request, stoppingToken);

                        if (foundRows > 0)
                        {
                            await fileImportStore.UpdateAsync(request.FileImportId.Value, Core.ApiClients.FileImportStatus.Completed, request.TotalRowsToProcess, foundRows, stoppingToken);
                        }
                        else
                        {
                            await fileImportStore.MarkFailedAsync(request.FileImportId.Value, stoppingToken);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to split file {Key}", request.SourceKey);
                        await fileImportStore.MarkFailedAsync(request.FileImportId.Value, stoppingToken);
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