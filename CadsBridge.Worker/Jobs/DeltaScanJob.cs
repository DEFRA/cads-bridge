using CadsBridge.Core.Locking;
using CadsBridge.Worker.Tasks;
using Microsoft.Extensions.Logging;
using Quartz;

namespace CadsBridge.Worker.Jobs;

public class DeltaScanJob(
    IDeltaFileScanTask deltaScanTask,
    IDistributedLock distributedLock,
    ILogger<DeltaScanJob> logger) : IJob
{
    private const string LockName = nameof(DeltaScanJob);

    public async Task Execute(IJobExecutionContext context)
    {
        if (!await distributedLock.TryAcquireAsync(LockName, context.CancellationToken))
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("Delta scan job skipped - lock {LockName} held by another instance", LockName);
            }
            return;
        }

        try
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("Delta scan job started");
            }

            await deltaScanTask.RunAsync(context.CancellationToken);

            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("Delta scan job completed");
            }
        }
        finally
        {
            await distributedLock.ReleaseAsync(LockName, context.CancellationToken);
        }
    }
}