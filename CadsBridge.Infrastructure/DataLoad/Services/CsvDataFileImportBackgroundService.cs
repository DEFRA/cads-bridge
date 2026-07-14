using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Messaging;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class CsvDataFileImportBackgroundService(
    Channel<CsvDataFileImportJob> channel,
    ILogger<CsvDataFileImportBackgroundService> logger,
    IFileImportStore fileImportStore,
    ISplitMessageProducer splitMessageProducer,
    IS3CopyService s3ExternalToInternalCopyService,
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
            try
            {
                fileImportId = await fileImportStore.Initiate(
                    request.SourceKey, 0, stoppingToken);
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
                        await fileImportStore.MarkInProgress(fileImportId, stoppingToken);

                        var result = await s3ExternalToInternalCopyService.ExecAsync(request, stoppingToken);

                        if (result)
                        {
                            await fileImportStore.MarkSucceeded(fileImportId, stoppingToken);

                            await splitMessageProducer.SendAsync(
                                new CsvDataFileSplitJob(
                                    JobId: request.JobId,
                                    SourceKey: request.TargetKey,
                                    FileImportId: fileImportId
                                ),
                                stoppingToken);
                        }
                        else
                        {
                            await fileImportStore.MarkFailed(fileImportId, stoppingToken);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to import {Key}", request.SourceKey);
                        await fileImportStore.MarkFailed(fileImportId, stoppingToken);
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