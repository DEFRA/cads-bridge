using CadsBridge.Core.Attributes;

namespace CadsBridge.Worker.Tasks;

public enum ScanTaskType
{
    [ScanTaskInfo("BULK", "cads/cts/bulk", "import/cts/bulk")]
    Bulk,
    [ScanTaskInfo("DELTA", "cads/cts/daily", "import/cts/daily")]
    Delta
}