using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Messaging;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Application.DataLoad.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class CsvDataFileImportBackgroundService(
    Channel<CsvDataFileImportJob> channel,
    ILogger<CsvDataFileImportBackgroundService> logger,
    IImportJobProgressStore progressStore,
    IFileImportStatusStore fileImportStatusStore,
    ISplitMessageProducer splitMessageProducer,
    IS3FileMetaDataService s3FileMetaDataService,
    IS3CopyService s3ExternalToInternalCopyService) : BackgroundService
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
                logger.LogInformation("Cancellation requested, aborting copy");
                return;
            }

            long fileImportStatusId;
            try
            {
                var totalRowsToProcess = await s3FileMetaDataService.GetRecordCountAsync(
                    request.SourceKey, stoppingToken);

                fileImportStatusId = await fileImportStatusStore.Initiate(
                    request.SourceKey, totalRowsToProcess, stoppingToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to initiate import for {Key}", request.SourceKey);
                progressStore.MarkFailed(request.JobId, request.SourceKey, ex.Message);
                continue;
            }

            await semaphore.WaitAsync(stoppingToken);

            var task = Task.Run(
                async () =>
                {
                    try
                    {
                        progressStore.MarkInProgress(request.JobId, request.SourceKey);
                        await fileImportStatusStore.MarkInProgress(fileImportStatusId, stoppingToken);

                        var result = await s3ExternalToInternalCopyService.ExecAsync(request, stoppingToken);

                        if (result)
                        {
                            progressStore.MarkSucceeded(request.JobId, request.SourceKey);

                            await splitMessageProducer.SendAsync(
                                new CsvDataFileSplitJob(
                                    JobId: request.JobId,
                                    SourceKey: request.TargetKey,
                                    FileImportStatusId: fileImportStatusId
                                ),
                                stoppingToken);
                        }
                        else
                        {
                            progressStore.MarkFailed(request.JobId, request.SourceKey, "Unknown error during copy");
                            await fileImportStatusStore.MarkFailed(fileImportStatusId, stoppingToken);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to import {Key}", request.SourceKey);
                        progressStore.MarkFailed(request.JobId, request.SourceKey, ex.Message);
                        await fileImportStatusStore.MarkFailed(fileImportStatusId, stoppingToken);
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