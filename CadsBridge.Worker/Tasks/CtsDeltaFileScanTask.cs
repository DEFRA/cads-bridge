using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Application.DataLoad.Services;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Worker.Tasks;

public class CtsDeltaFileScanTask(
    IFileDiscoveryService fileDiscoveryService,
    ILogger<CtsDeltaFileScanTask> logger
    ) : FileScanTask(DataSourceType.CtsDelta, fileDiscoveryService, logger)
{
}