using System.Collections.Concurrent;
using System.Threading.Channels;
using CadsBridge.Application.Models;
using CadsBridge.Application.Persistance;
using CadsBridge.Application.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Application.FileImport.Services;

public class FileImportBackgroundService(
    Channel<FileImportJob> channel,
    ILogger<FileImportBackgroundService> logger,
    IImportJobProgressStore progressStore,
    ISplitMessageProducer splitMessageProducer,
    IS3ExternalToInternalCopyService s3ExternalToInternalCopyService) : BackgroundService
{
    private readonly Channel<FileImportJob> _channel = channel;
    private readonly ILogger<FileImportBackgroundService> _logger = logger;
    private readonly IImportJobProgressStore _progressStore = progressStore;
    private readonly ISplitMessageProducer _splitMessageProducer = splitMessageProducer;
    private readonly IS3ExternalToInternalCopyService _s3ExternalToInternalCopyService = s3ExternalToInternalCopyService;
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
                                    new FileSplitJob(
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