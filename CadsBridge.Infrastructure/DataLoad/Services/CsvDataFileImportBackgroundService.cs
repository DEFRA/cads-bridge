using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Messaging;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Core.DataLoad.Jobs;
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

            await semaphore.WaitAsync(stoppingToken);

            var task = Task.Run(
                async () =>
                {
                    try
                    {
                        progressStore.MarkInProgress(request.JobId, request.SourceKey);
                        await fileImportStatusStore.MarkInProgress(request.FileImportStatusId, stoppingToken);

                        var result = await s3ExternalToInternalCopyService.ExecAsync(request, stoppingToken);

                        if (result)
                        {
                            progressStore.MarkSucceeded(request.JobId, request.SourceKey);

                            var targetFolder = $"import/{Path.GetFileNameWithoutExtension(request.TargetKey)}";

                            await splitMessageProducer.SendAsync(
                                new CsvDataFileSplitJob(
                                    JobId: request.JobId,
                                    Key: request.TargetKey,
                                    TargetFolder: targetFolder,
                                    SplitType: request.SplitType,
                                    SplitValue: request.SplitValue,
                                    FileImportStatusId: request.FileImportStatusId
                                ),
                                stoppingToken);
                        }
                        else
                        {
                            progressStore.MarkFailed(request.JobId, request.SourceKey, "Unknown error during copy");
                            await fileImportStatusStore.MarkFailed(request.FileImportStatusId, stoppingToken);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to import {Key}", request.SourceKey);
                        progressStore.MarkFailed(request.JobId, request.SourceKey, ex.Message);
                        await fileImportStatusStore.MarkFailed(request.FileImportStatusId, stoppingToken);
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