using CadsBridge.Core.Locking;
using Microsoft.Extensions.Logging;
using Quartz;
using System.Diagnostics.CodeAnalysis;
using CadsBridge.Worker.Tasks;

namespace CadsBridge.Worker.Jobs;

[ExcludeFromCodeCoverage] // Exclude until actual implementation is added
public class BulkScanJob(
    BulkScanTask bulkScanTask,
    IDistributedLock distributedLock,
    ILogger<BulkScanJob> logger) : IJob
{
    private const string LockName = nameof(BulkScanJob);

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