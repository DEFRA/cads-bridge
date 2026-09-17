using CadsBridge.Core.Attributes;

namespace CadsBridge.Application.DataLoad.Scanning;

public enum DataSourceType
{
    [ScanTaskInfo("BULK", "cads/cts/bulk", "import/cts/bulk")]
    CtsBulk,
    [ScanTaskInfo("DELTA", "cads/cts/daily", "import/cts/daily")]
    CtsDelta
}