using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Messaging;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;

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

            long fileImportId;
            using var scope = serviceScopeFactory.CreateScope();
            var fileImportStore = scope.ServiceProvider.GetRequiredService<IFileImportStore>();
            var s3ExternalToInternalCopyService = scope.ServiceProvider.GetRequiredService<IS3CopyService>();
            var splitMessageProducer = scope.ServiceProvider.GetRequiredService<ISplitMessageProducer>();
            try
            {
                fileImportId = await fileImportStore.CreateAsync(request.SourceKey, cancellationToken: stoppingToken);
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
                            await fileImportStore.UpdateAsync(fileImportId, Core.ApiClients.FileImportStatus.Completed, totalRowsToProcess, cancellationToken: stoppingToken);

                            await splitMessageProducer.SendAsync(
                                new CsvDataFileSplitJob(request.TargetKey, fileImportId, totalRowsToProcess),
                                stoppingToken);
                        }
                        else
                        {
                            await fileImportStore.MarkFailedAsync(fileImportId, stoppingToken);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to import {Key}", request.SourceKey);
                        await fileImportStore.MarkFailedAsync(fileImportId, stoppingToken);
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