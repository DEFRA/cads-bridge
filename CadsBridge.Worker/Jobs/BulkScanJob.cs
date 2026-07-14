using Microsoft.Extensions.Logging;
using Quartz;

namespace CadsBridge.Worker.Jobs;

public class BulkScanJob(ILogger<BulkScanJob> logger) : IJob
{
    public Task Execute(IJobExecutionContext context)
    {
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("Bulk scan job started");
        }
        return Task.CompletedTask;
    }
}