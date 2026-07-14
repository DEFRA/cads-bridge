using CadsBridge.Core.Locking;
using Microsoft.Extensions.Logging;
using Quartz;

namespace CadsBridge.Worker.Jobs;

public class DeltaScanJob(IDistributedLock distributedLock, ILogger<DeltaScanJob> logger) : IJob
{
    private const string LockName = nameof(DeltaScanJob);

    public async Task Execute(IJobExecutionContext context)
    {
        if (!await distributedLock.TryAcquireAsync(LockName, context.CancellationToken))
        {
            logger.LogInformation("Delta scan job skipped - lock {LockName} held by another instance", LockName);
            return;
        }

        try
        {
            logger.LogInformation("Delta scan job started");
            // ... job work ...
        }
        finally
        {
            await distributedLock.ReleaseAsync(LockName, context.CancellationToken);
        }
    }
}