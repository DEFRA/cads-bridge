using CadsBridge.Core.Locking;
using Microsoft.Extensions.Logging;
using Quartz;
using System.Diagnostics.CodeAnalysis;

namespace CadsBridge.Worker.Jobs;

[ExcludeFromCodeCoverage] // Exclude until actual implementation is added
public class BulkScanJob(IDistributedLock distributedLock, ILogger<BulkScanJob> logger) : IJob
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
            // ... job work ...
        }
        finally
        {
            await distributedLock.ReleaseAsync(LockName, context.CancellationToken);
        }
    }
}