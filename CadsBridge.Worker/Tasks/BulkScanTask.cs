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

        // Filter for valid bulk file names
        var validFilesKeys = result.Where(GetValidBulkFileNames).ToList();
        if (validFilesKeys.Count == 0)
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("No valid bulk import filenames found in the external bucket.");
            }
            return;
        }

        // Validate if processed or complete already, if so ignore
        var keysToIgnore = new List<string>();
        foreach (var fileName in validFilesKeys)
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
            validFilesKeys.RemoveAll(keysToIgnore.Contains);
        }

        // Send unprocessed keys to the queue
        if (validFilesKeys.Count == 0)
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("No files remain to enqueue after filtering.");
            }
            return;
        }

        await fileDiscoveryService.EnQueueFileImportMessages(validFilesKeys, cancellationToken);

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("Enqueued {Count} file import messages.", validFilesKeys.Count);
        }
    }

    private bool GetValidBulkFileNames(string fileName)
    {
        return CtsmFilenameParser.TryParse(fileName, out var parsed) &&
               parsed!.Type.Equals("BULK", StringComparison.OrdinalIgnoreCase);
    }
}