using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Core.Locking;
using CadsBridge.Worker.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Quartz;

namespace CadsBridge.Worker.Jobs;

public class CtsDeltaScanJob(
    [FromKeyedServices(DataSourceType.CtsDelta)] IFileScanTask deltaScanTask,
    IDistributedLock distributedLock,
    ILogger<CtsDeltaScanJob> logger) : IJob
{
    private const string LockName = nameof(CtsDeltaScanJob);

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