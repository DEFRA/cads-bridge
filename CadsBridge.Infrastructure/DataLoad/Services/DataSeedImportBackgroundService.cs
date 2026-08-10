using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Core.Correlation;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class DataSeedImportBackgroundService(
    Channel<DataSeedFileLoadJob> channel,
    IFileSystemToS3CopyService fileSystemToS3CopyService,
    ILogger<DataSeedImportBackgroundService> logger) : BackgroundService
{
    private readonly int _maxParallelFileTransfers = 4;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var semaphore = new SemaphoreSlim(_maxParallelFileTransfers);
        var runningTasks = new ConcurrentBag<Task>();

        await foreach (var request in channel.Reader.ReadAllAsync(stoppingToken))
        {
            if (stoppingToken.IsCancellationRequested)
            {
                logger.LogInformation("Cancellation requested, aborting split");
                return;
            }

            await semaphore.WaitAsync(stoppingToken);

            var task = Task.Run(async () =>
            {
                // Establish the correlation id for this unit of work based on what was used for file discovery
                // This keeps it consistent across the various processes for this particular file import
                CorrelationIdContext.Value = string.IsNullOrWhiteSpace(request.CorrelationId)
                    ? Guid.NewGuid().ToString()
                    : request.CorrelationId;

                using (logger.BeginScope(new Dictionary<string, object?>
                {
                    ["CorrelationId"] = CorrelationIdContext.Value
                }))
                {
                    try
                    {
                        var result = await fileSystemToS3CopyService.ExecuteAsync(request, stoppingToken);
                        if (result)
                        {
                            if (logger.IsEnabled(LogLevel.Information))
                                logger.LogInformation("Successfully imported data seed file {FileName} to {TargetKey}", request.FileName, request.TargetKey);
                        }
                        else
                        {
                            logger.LogError("Failed to import data seed file {FileName} to {TargetKey}", request.FileName, request.TargetKey);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to import data seed file {FileName} to {TargetKey}", request.FileName, request.TargetKey);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }
            }, stoppingToken);
            runningTasks.Add(task);
        }
        await Task.WhenAll(runningTasks);
    }
}