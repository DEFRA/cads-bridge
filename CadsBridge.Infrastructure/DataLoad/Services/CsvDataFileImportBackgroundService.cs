using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Messaging;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Core.ApiClients;
using CadsBridge.Core.Correlation;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class CsvDataFileImportBackgroundService(
    Channel<CsvDataFileImportJob> channel,
    ILogger<CsvDataFileImportBackgroundService> logger,
    IServiceScopeFactory serviceScopeFactory,
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
                logger.LogInformation("Cancellation requested, aborting copy");
                return;
            }

            // Establish the correlation id for this unit of work based on what was used for file discovery
            // This keeps it consistent across the various processes for this particular file import
            CorrelationIdContext.Value = string.IsNullOrWhiteSpace(request.CorrelationId)
                ? Guid.NewGuid().ToString()
                : request.CorrelationId;

            long fileImportId;
            using var scope = serviceScopeFactory.CreateScope();
            var fileImportStore = scope.ServiceProvider.GetRequiredService<IFileImportStore>();
            var s3ExternalToInternalCopyService = scope.ServiceProvider.GetRequiredService<IS3CopyService>();
            var splitMessageProducer = scope.ServiceProvider.GetRequiredService<ISplitMessageProducer>();
            try
            {
                fileImportId = await fileImportStore.CreateAsync(request.SourceKeyFileName, cancellationToken: stoppingToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to initiate import for {Key}", request.SourceKey);
                continue;
            }

            await semaphore.WaitAsync(stoppingToken);

            var task = Task.Run(
                async () =>
                {
                    try
                    {
                        var totalRowsToProcess = await s3ExternalToInternalCopyService.ExecAsync(request, stoppingToken);

                        if (totalRowsToProcess > 0)
                        {
                            await fileImportStore.UpdateAsync(fileImportId, FileImportStatus.Transferred, totalRowsToProcess, cancellationToken: stoppingToken);

                            await splitMessageProducer.SendAsync(
                                new CsvDataFileSplitJob(request.TargetKey, fileImportId, totalRowsToProcess, request.CorrelationId),
                                stoppingToken);
                        }
                        else
                        {
                            await fileImportStore.MarkFailedAsync(fileImportId, "Import failed: No rows to process", stoppingToken);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to import {Key}", request.SourceKey);
                        await fileImportStore.MarkFailedAsync(fileImportId, $"Import failed: {ex.Message}", stoppingToken);
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