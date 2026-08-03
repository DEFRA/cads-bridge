using CadsBridge.Core.Correlation;
using CadsBridge.Core.Locking;
using CadsBridge.Worker.Tasks;
using Microsoft.Extensions.Logging;
using Quartz;

namespace CadsBridge.Worker.Jobs;

public class BulkScanJob(
    IBulkScanTask bulkScanTask,
    IDistributedLock distributedLock,
    ILogger<BulkScanJob> logger) : IJob
{
    private const string LockName = nameof(BulkScanJob);

    public async Task Execute(IJobExecutionContext context)
    {
        // The correlation middleware does not run for Quartz jobs, so seed a correlation id at the
        // start of the run. It flows through the scan/discovery async chain and is serialized onto
        // the messages published to the queue.
        CorrelationIdContext.Value ??= Guid.NewGuid().ToString();

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

            await bulkScanTask.RunAsync(context.CancellationToken);

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