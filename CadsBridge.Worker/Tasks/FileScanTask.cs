using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Application.Extensions;
using CadsBridge.Core.Attributes;
using CadsBridge.Infrastructure.DataLoad.Sources;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Worker.Tasks;

public abstract class FileScanTask(
    DataSourceType dataSourceType,
    IFileDiscoveryService fileDiscoveryService,
    ILogger<FileScanTask> logger
    ) : IFileScanTask
{
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        // Retrieve the list of files from the external bucket based on the data source type prefix if provided
        var scanTaskInfo = dataSourceType.GetAttribute<DataSourceTypeInfoAttribute>();
        var dataSourceTypePrefix = scanTaskInfo?.Prefix;
        var dataSourceTypeName = scanTaskInfo?.Name;
        var destinationPrefix = scanTaskInfo?.DestinationPrefix
            ?? throw new InvalidOperationException($"Data source type '{dataSourceType}' has no destination prefix configured.");

        // Get the list of files in the external bucket
        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug("Starting {DataSourceTypeName} scan task ...", dataSourceTypeName);
        }

        var result = await fileDiscoveryService.GetFileNames(dataSourceTypePrefix, cancellationToken);

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

        var validObjectKeys = objectKeys.Where(fk => ValidateFileKey(dataSourceType, fk)).ToList();

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

    private bool ValidateFileKey(DataSourceType dataSourceType, string objectKey)
    {
        var name = dataSourceType.GetAttribute<DataSourceTypeInfoAttribute>()?.Name
            ?? throw new InvalidOperationException($"Data source type '{dataSourceType}' has no name configured.");

        var fileName = Path.GetFileName(objectKey);

        return DataSourceStrategyFactory.Create(dataSourceType).IsFileNameValidForType(fileName, name);
    }
}