using CadsBridge.Application.DataLoad.Services;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Worker.Tasks;

public class DeltaFileScanTask(
    IFileDiscoveryService fileDiscoveryService,
    ILogger<DeltaFileScanTask> logger
    ) : FileScanTask(ScanTaskType.Delta, fileDiscoveryService, logger), IDeltaFileScanTask
{
}