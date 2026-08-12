using CadsBridge.Application.DataLoad.Services;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Worker.Tasks;

public class BulkFileScanTask(
    IFileDiscoveryService fileDiscoveryService,
    ILogger<BulkFileScanTask> logger
    ) : FileScanTask(ScanTaskType.Bulk, fileDiscoveryService, logger), IBulkFileScanTask
{
}