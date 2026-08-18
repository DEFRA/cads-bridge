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
using CadsBridge.Core.Exceptions;

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

        try
        {
            // Drain the channel rather than aborting: WaitToReadAsync observes the stopping token,
            // but we deliberately do NOT abandon in-flight work. When cancellation is requested we
            // stop accepting new jobs and fall through to Task.WhenAll so already-started imports
            // can finish (or run their mark-as-failed path) before the host tears the process down.
            while (await channel.Reader.WaitToReadAsync(stoppingToken))
            {
                while (channel.Reader.TryRead(out var request))
                {
                    await semaphore.WaitAsync(stoppingToken);

                    var task = Task.Run(
                        async () =>
                        {
                            using var scope = serviceScopeFactory.CreateScope();
                            var fileImportStore = scope.ServiceProvider.GetRequiredService<IFileImportStore>();
                            var s3ExternalToInternalCopyService = scope.ServiceProvider.GetRequiredService<IS3CopyService>();
                            var splitMessageProducer = scope.ServiceProvider.GetRequiredService<ISplitMessageProducer>();

                            using (CorrelationScope.Begin(request.CorrelationId))
                            {
                                try
                                {
                                    var fileImportId = await CreateFileImportRecord(fileImportStore, request, stoppingToken);
                                    if (fileImportId is null)
                                    {
                                        return; // File import record not created
                                    }
                                    await ProcessFileTransferAndDecrypt(s3ExternalToInternalCopyService, request, fileImportId.Value, fileImportStore, splitMessageProducer, stoppingToken);
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
            }
        }
        catch (OperationCanceledException ex)
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation(ex, "Shutdown requested; waiting for in-flight imports to finalise");
            }
        }

        await Task.WhenAll(runningTasks);
    }

    private async Task ProcessFileTransferAndDecrypt(
        IS3CopyService s3ExternalToInternalCopyService,
        CsvDataFileImportJob request,
        long fileImportId,
        IFileImportStore fileImportStore,
        ISplitMessageProducer splitMessageProducer,
        CancellationToken stoppingToken)
    {
        try
        {
            var totalRowsToProcess = await ProcessRequest(s3ExternalToInternalCopyService, request,
                fileImportId, stoppingToken);
            await fileImportStore.UpdateAsync(fileImportId, FileImportStatus.Transferred,
                totalRowsToProcess, cancellationToken: stoppingToken);

            await splitMessageProducer.SendAsync(
                new CsvDataFileSplitJob(request.TargetKey, fileImportId, totalRowsToProcess,
                    request.CorrelationId),
                stoppingToken);
        }
        catch (Exception ex)
        {
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.LogError(ex, "Failed to import {Key}", request.SourceKey);
            }
            await MarkFailedWithinGracePeriodAsync(fileImportStore, fileImportId,
                $"Import failed: {ex.Message}");
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
                    "Failed to mark import {FileImportId} as failed", fileImportId);
            }
        }
    }

    private async Task<long?> CreateFileImportRecord(
        IFileImportStore fileImportStore,
        CsvDataFileImportJob request,
        CancellationToken stoppingToken)
    {
        long fileImportId;
        try
        {
            fileImportId = await fileImportStore.CreateAsync(request.SourceKeyFileName,
                cancellationToken: stoppingToken);
        }
        catch (BusinessRuleValidationException ex)
        {
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.LogError(ex, "Skipped import for {Key}", request.SourceKey);
            }
            return null;
        }
        catch (Exception ex)
        {
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.LogError(ex, "Failed to initiate import for {Key}", request.SourceKey);
            }
            return null;
        }

        return fileImportId;
    }

    private async Task<long> ProcessRequest(IS3CopyService s3ExternalToInternalCopyService, CsvDataFileImportJob request, long fileImportId, CancellationToken stoppingToken)
    {
        var totalRowsToProcess = await s3ExternalToInternalCopyService.ExecAsync(request, stoppingToken);
        if (logger.IsEnabled(LogLevel.Information))
        {
            if (totalRowsToProcess > 0)
            {
                logger.LogInformation(
                    "File import {FileImportId} for {Key} transferred successfully with {RowCount} rows to process",
                    fileImportId,
                    request.SourceKey,
                    totalRowsToProcess);
            }
            else
            {
                logger.LogInformation(
                    "File import {FileImportId} for {Key} transferred successfully. No row count provided",
                    fileImportId,
                    request.SourceKey);
            }
        }
        return totalRowsToProcess;
    }
}