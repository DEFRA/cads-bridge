using CadsBridge.Core.Attributes;
using System.ComponentModel;

namespace CadsBridge.Worker.Tasks;

public enum ScanTaskType
{
    [ScanTaskInfo("BULK", "bulk")]
    Bulk,
    [ScanTaskInfo("DELTA", "daily")]
    Delta
}