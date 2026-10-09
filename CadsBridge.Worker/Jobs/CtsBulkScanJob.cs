using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Core.Locking;
using CadsBridge.Worker.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Quartz;

namespace CadsBridge.Worker.Jobs;

public class CtsBulkScanJob(
    [FromKeyedServices(DataSourceType.CtsBulk)] IFileScanTask bulkScanTask,
    IDistributedLock distributedLock,
    ILogger<CtsBulkScanJob> logger) : IJob
{
    private const string LockName = nameof(CtsBulkScanJob);

    public async Task Execute(IJobExecutionContext context)
    {
        if (!await distributedLock.TryAcquireAsync(LockName, context.CancellationToken))
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("Bulk scan job skipped - lock {LockName} held by another instance", LockName);
            }
            return;
        }

        try
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("Bulk scan job started");
            }

            var completed = await distributedLock.RunWithRenewalAsync(
                LockName, logger, bulkScanTask.RunAsync, context.CancellationToken);

            if (!completed)
            {
                logger.LogWarning("Delta scan job aborted - lock {LockName} lease was lost", LockName);
                return;
            }

            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("Bulk scan job completed");
            }
        }
        finally
        {
            await distributedLock.ReleaseAsync(LockName, context.CancellationToken);
        }
    }
}