using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Application.Extensions;
using CadsBridge.Core.Attributes;
using CadsBridge.Infrastructure.DataLoad.Csv.Files;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Worker.Tasks;

public abstract class FileScanTask(
    ScanTaskType scanTaskType,
    IFileDiscoveryService fileDiscoveryService,
    ILogger<FileScanTask> logger
    ) : IFileScanTask
{
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        // Retrieve the list of files from the external bucket based on the scan type prefix if provided
        var scanTaskInfo = scanTaskType.GetAttribute<ScanTaskInfoAttribute>();
        var scanTaskTypePrefix = scanTaskInfo?.Prefix;
        var scanTaskTypeName = scanTaskInfo?.Name;
        var destinationPrefix = scanTaskInfo?.DestinationPrefix
            ?? throw new InvalidOperationException($"Scan task type '{scanTaskType}' has no destination prefix configured.");

        // Get the list of files in the external bucket
        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug("Starting {ScanTaskTypeName} scan task ...", scanTaskTypeName);
        }

        var result = await fileDiscoveryService.GetFileNames(scanTaskTypePrefix, cancellationToken);

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
        await fileDiscoveryService.EnQueueFileImportMessages(validFileKeys, destinationPrefix, cancellationToken);

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("Enqueued {Count} file import messages.", validFileKeys.Count);
        }
    }

    private async Task<List<string>> GetKeysToEnqueue(List<string> objectKeys, CancellationToken cancellationToken)
    {
        var keysToProcess = new List<string>();

        var validObjectKeys = objectKeys.Where(fk => ValidateFileKey(scanTaskType, fk)).ToList();

        foreach (var objectKey in validObjectKeys)
        {
            var fileName = Path.GetFileName(objectKey);
            if (await fileDiscoveryService.IsFileValid(fileName, cancellationToken))
            {
                keysToProcess.Add(objectKey);
            }
        }

        return keysToProcess;
    }

    private static bool ValidateFileKey(ScanTaskType scanTaskType, string objectKey)
    {
        var name = scanTaskType.GetAttribute<ScanTaskInfoAttribute>()?.Name;

        var fileName = Path.GetFileName(objectKey);

        return CtsmFilenameParser.TryParse(fileName, out var parsed) &&
            parsed!.Type.Equals(name, StringComparison.OrdinalIgnoreCase);
    }
}