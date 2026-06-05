using CadsBridge.Application.Models;
using CadsBridge.Application.Persistance;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace CadsBridge.Application.Services;

public class FileImportBackgroundService(
    Channel<FileImportJob> channel,
    ILogger<FileImportBackgroundService> logger,
    IImportJobProgressStore progressStore,
    ISplitMessageProducer splitMessageProducer,
    IFileImportCopyService fileImportCopyService) : BackgroundService
{
    private readonly Channel<FileImportJob> _channel = channel;
    private readonly ILogger<FileImportBackgroundService> _logger = logger;
    private readonly IImportJobProgressStore _progressStore = progressStore;
    private readonly ISplitMessageProducer _splitMessageProducer = splitMessageProducer;
    private readonly IFileImportCopyService _fileImportCopyService = fileImportCopyService;
    private readonly int _maxParallelDownloads = 4;

    protected override async Task ExecuteAsync(CancellationToken cancellationToken = default)
    {
        var semaphore = new SemaphoreSlim(_maxParallelDownloads);
        var runningTasks = new ConcurrentBag<Task>();

        await foreach (var request in _channel.Reader.ReadAllAsync(cancellationToken))
        {
            if (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("Cancellation requested, aborting copy");
                return;
            }

            await semaphore.WaitAsync(cancellationToken);

            var task = Task.Run(
                async () =>
                {
                    try
                    {
                        _progressStore.MarkInProgress(request.JobId, request.SourceKey);
                        var result = await _fileImportCopyService.CopyWithRetryAsync(request, cancellationToken);

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
                                    cancellationToken);
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
                cancellationToken);

            runningTasks.Add(task);
        }

        await Task.WhenAll(runningTasks);
    }
}