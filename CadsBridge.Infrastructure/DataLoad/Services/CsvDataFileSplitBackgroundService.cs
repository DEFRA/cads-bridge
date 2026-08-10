using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Core.ApiClients;
using CadsBridge.Core.Correlation;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class CsvDataFileSplitBackgroundService(
    Channel<CsvDataFileSplitJob> channel,
    ILogger<CsvDataFileSplitBackgroundService> logger,
    IServiceScopeFactory scopeFactory,
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
                logger.LogError("FileImportId is required for split job {Key}, CorrelationId {CorrelationId}", request.SourceKey, request.CorrelationId);
                continue;
            }

            await semaphore.WaitAsync(stoppingToken);

            var task = Task.Run(
                async () =>
                {
                    using var scope = scopeFactory.CreateScope();
                    var fileImportStore = scope.ServiceProvider.GetRequiredService<IFileImportStore>();
                    var csvDataFileSplitterService = scope.ServiceProvider.GetRequiredService<ICsvDataFileSplitterService>();

                    // Establish the correlation id for this unit of work based on what was used for file discovery
                    // This keeps it consistent across the various processes for this particular file import
                    CorrelationIdContext.Value = string.IsNullOrWhiteSpace(request.CorrelationId)
                        ? Guid.NewGuid().ToString()
                        : request.CorrelationId;

                    using (logger.BeginScope(new Dictionary<string, object?>
                    {
                        ["CorrelationId"] = CorrelationIdContext.Value
                    }))
                    {
                        try
                        {
                            var foundRows = await csvDataFileSplitterService.ExecuteAsync(request, stoppingToken);

                            if (foundRows > 0)
                            {
                                await fileImportStore.UpdateAsync(request.FileImportId.Value, FileImportStatus.Split, request.TotalRowsToProcess, foundRows, stoppingToken);
                            }
                            else
                            {
                                await fileImportStore.MarkFailedAsync(request.FileImportId.Value, $"Split failed: No rows to process", stoppingToken);
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Failed to split file {Key}", request.SourceKey);
                            await fileImportStore.MarkFailedAsync(request.FileImportId.Value, $"Split failed: {ex.Message}", stoppingToken);
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }
                },
                stoppingToken);

            runningTasks.Add(task);
        }

        await Task.WhenAll(runningTasks);
    }
}