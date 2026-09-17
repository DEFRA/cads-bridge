using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Application.DataLoad.Services;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Worker.Tasks;

public class CtsBulkFileScanTask(
    IFileDiscoveryService fileDiscoveryService,
    ILogger<CtsBulkFileScanTask> logger
    ) : FileScanTask(DataSourceType.CtsBulk, fileDiscoveryService, logger)
{
}