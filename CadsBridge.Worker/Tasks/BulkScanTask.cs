using CadsBridge.Application.DataLoad.Services;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Worker.Tasks;

public class BulkScanTask(
    IFileDiscoveryService fileDiscoveryService,
    ILogger<BulkScanTask> logger
    ) : IBulkScanTask
{
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        // Get the list of files in the external bucket
        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug("Starting bulk scan task...");
        }
        var result = await fileDiscoveryService.GetFileNames(cancellationToken);

        // Validate if processed already

        // Send unprocessed keys to the queue
    }
}