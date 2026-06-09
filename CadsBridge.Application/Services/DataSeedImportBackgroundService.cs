using System.Collections.Concurrent;
using System.Threading.Channels;
using CadsBridge.Application.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Application.Services;

public class DataSeedImportBackgroundService(Channel<DataSeedImportJob> channel, ILogger<DataSeedImportBackgroundService> logger, IDataSeedFileCopyService dataSeedFileCopyService) : BackgroundService
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
                try
                {
                    var result = await dataSeedFileCopyService.ExecuteAsync(request, stoppingToken);
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
            }, stoppingToken);
            runningTasks.Add(task);
        }
        await Task.WhenAll(runningTasks);
    }
}