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

        if (result.Count == 0)
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("No files found in the external bucket.");
            }
            return;
        }

        // Filter for valid file names
        var validFileKeys = await GetKeysToEnqueue(result, cancellationToken);

        if (validFileKeys.Count == 0)
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("No files remain to enqueue after filtering.");
            }
            return;
        }

        // Send file names to the queue for processing
        await fileDiscoveryService.EnQueueFileImportMessages(validFileKeys, cancellationToken);

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("Enqueued {Count} file import messages.", validFileKeys.Count);
        }
    }

    private async Task<List<string>> GetKeysToEnqueue(List<string> result, CancellationToken cancellationToken)
    {
        var validFileKeys = result.Where(GetValidBulkFileNames).ToList();
        if (validFileKeys.Count == 0)
        {
            return validFileKeys;
        }

        // Validate if processed or complete already, if so ignore
        var keysToIgnore = new List<string>();
        foreach (var fileName in validFileKeys)
        {
            if (!await fileDiscoveryService.IsFileValid(fileName, cancellationToken))
            {
                keysToIgnore.Add(fileName);
            }
        }

        if (keysToIgnore.Count > 0)
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("Ignoring {Count} files that have already been processed or failed too many times: {Files}", keysToIgnore.Count, string.Join(", ", keysToIgnore));
            }
            validFileKeys.RemoveAll(keysToIgnore.Contains);
        }

        return validFileKeys;
    }

    private bool GetValidBulkFileNames(string fileName)
    {
        return CtsmFilenameParser.TryParse(fileName, out var parsed) &&
               parsed!.Type.Equals("BULK", StringComparison.OrdinalIgnoreCase);
    }
}