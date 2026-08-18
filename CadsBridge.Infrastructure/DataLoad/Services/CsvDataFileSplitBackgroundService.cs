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

        try
        {
            // Drain the channel rather than aborting: WaitToReadAsync observes the stopping token,
            // but we deliberately do NOT abandon in-flight work. When cancellation is requested we
            // stop accepting new jobs and fall through to Task.WhenAll so already-started splits can
            // finish (or run their mark-as-failed path) before the host tears the process down.
            while (await channel.Reader.WaitToReadAsync(stoppingToken))
            {
                while (channel.Reader.TryRead(out var request))
                {
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

                            using (CorrelationScope.Begin(request.CorrelationId))
                            {
                                await ProcessCsvDataFileSplit(csvDataFileSplitterService, request, fileImportStore, semaphore, stoppingToken);
                            }
                        },
                        stoppingToken);

                    runningTasks.Add(task);
                }
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Shutdown requested; waiting for in-flight splits to finalise");
        }

        await Task.WhenAll(runningTasks);
    }

    private async Task ProcessCsvDataFileSplit(ICsvDataFileSplitterService csvDataFileSplitterService,
        CsvDataFileSplitJob request, IFileImportStore fileImportStore, SemaphoreSlim semaphore,
        CancellationToken stoppingToken)
    {
        try
        {
            var foundRows = await csvDataFileSplitterService.ExecuteAsync(request, stoppingToken);

            if (foundRows > 0)
            {
                await fileImportStore.UpdateAsync(request.FileImportId!.Value, FileImportStatus.Split, request.TotalRowsToProcess, foundRows, stoppingToken);
            }
            else
            {
                await fileImportStore.MarkFailedAsync(request.FileImportId!.Value, $"Split failed: No rows to process", stoppingToken);
            }
        }
        catch (Exception ex)
        {
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.LogError(ex, "Failed to split file {Key}", request.SourceKey);
            }
            await MarkFailedWithinGracePeriodAsync(fileImportStore, request.FileImportId!.Value,
                $"Split failed: {ex.Message}");
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task MarkFailedWithinGracePeriodAsync(
        IFileImportStore fileImportStore,
        long fileImportId,
        string reason)
    {
        // Use a fresh, bounded token that is independent of the host stopping token so the
        // mark-as-failed API call is neither pre-empted by shutdown nor able to hang the
        // shutdown indefinitely if the API is slow/unresponsive.
        using var markCts = new CancellationTokenSource(
            TimeSpan.FromSeconds(config.MarkFailedTimeoutSeconds));
        try
        {
            await fileImportStore.MarkFailedAsync(fileImportId, reason, markCts.Token);
        }
        catch (Exception markEx)
        {
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.LogError(markEx,
                    "Failed to mark split {FileImportId} as failed", fileImportId);
            }
        }
    }
}