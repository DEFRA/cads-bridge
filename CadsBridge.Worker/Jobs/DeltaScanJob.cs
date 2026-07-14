using Microsoft.Extensions.Logging;
using Quartz;

namespace CadsBridge.Worker.Jobs;

public class DeltaScanJob(ILogger<DeltaScanJob> logger) : IJob
{
    public Task Execute(IJobExecutionContext context)
    {
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("Delta scan job started");
        }
        return Task.CompletedTask;
    }
}