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
        // Background jobs run outside the HTTP pipeline, so no middleware sets the
        // correlation ID.  Generate one per job execution so it flows through into
        // every SQS message published by this run.
        CorrelationIdContext.Value = Guid.NewGuid().ToString();

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