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
    ISplitMessageProducer splitMessageProducer,
    IS3CopyService s3ExternalToInternalCopyService) : BackgroundService
{
    private readonly Channel<CsvDataFileImportJob> _channel = channel;
    private readonly ILogger<CsvDataFileImportBackgroundService> _logger = logger;
    private readonly IImportJobProgressStore _progressStore = progressStore;
    private readonly ISplitMessageProducer _splitMessageProducer = splitMessageProducer;
    private readonly IS3CopyService _s3ExternalToInternalCopyService = s3ExternalToInternalCopyService;
    private readonly int _maxParallelDownloads = 4;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var semaphore = new SemaphoreSlim(_maxParallelDownloads);
        var runningTasks = new ConcurrentBag<Task>();

        await foreach (var request in _channel.Reader.ReadAllAsync(stoppingToken))
        {
            if (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Cancellation requested, aborting copy");
                return;
            }

            await semaphore.WaitAsync(stoppingToken);

            var task = Task.Run(
                async () =>
                {
                    try
                    {
                        _progressStore.MarkInProgress(request.JobId, request.SourceKey);
                        var result = await _s3ExternalToInternalCopyService.ExecAsync(request, stoppingToken);

                        if (result)
                        {
                            _progressStore.MarkSucceeded(request.JobId, request.SourceKey);

                            if (request.SplitType != SplitType.None)
                            {
                                await _splitMessageProducer.SendAsync(
                                    new CsvDataFileSplitJob(
                                        JobId: request.JobId,
                                        Key: request.TargetKey,
                                        TargetFolder: Path.GetFileNameWithoutExtension(request.TargetKey),
                                        SplitType: request.SplitType,
                                        SplitValue: request.SplitValue
                                    ),
                                    stoppingToken);
                            }
                        }
                        else
                        {
                            _progressStore.MarkFailed(request.JobId, request.SourceKey, "Unknown error during copy");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to import {Key}", request.SourceKey);
                        _progressStore.MarkFailed(request.JobId, request.SourceKey, ex.Message);
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