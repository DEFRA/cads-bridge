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
                        long fileImportId;
                        try
                        {
                            fileImportId = await fileImportStore.CreateAsync(request.SourceKeyFileName, cancellationToken: stoppingToken);
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Failed to initiate import for {Key}", request.SourceKey);
                            return;
                        }

                        try
                        {
                            var totalRowsToProcess = await ProcessRequest(s3ExternalToInternalCopyService, request, fileImportId, stoppingToken);
                            await fileImportStore.UpdateAsync(fileImportId, FileImportStatus.Transferred, totalRowsToProcess, cancellationToken: stoppingToken);

                            await splitMessageProducer.SendAsync(
                                new CsvDataFileSplitJob(request.TargetKey, fileImportId, totalRowsToProcess, request.CorrelationId),
                                stoppingToken);
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
                    }
                },
                stoppingToken);

            runningTasks.Add(task);
        }
        await Task.WhenAll(runningTasks);
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