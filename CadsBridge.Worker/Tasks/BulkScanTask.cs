using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Infrastructure.DataLoad.Csv.Files;
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

        if(result.Count == 0)
        {
            if(logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("No files found in the external bucket.");
            }
            return;
        }

        // Validate if processed already
        var validFilesKeys = result.Where(GetValidBulkFileNames).ToList();
        if(validFilesKeys.Count == 0)
        {
            if(logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("No valid bulk import filenames found in the external bucket.");
            }
            return;
        }

        // Send unprocessed keys to the queue
    }

    private bool GetValidBulkFileNames(string fileName)
    {
        return CtsmFilenameParser.TryParse(fileName, out var parsed) &&
               parsed!.Type.Equals("BULK", StringComparison.OrdinalIgnoreCase);
    }
}