using CadsBridge.Core.Attributes;

namespace CadsBridge.Application.DataLoad.Scanning;

public enum DataSourceType
{
    [DataSourceTypeInfo("BULK", "cads/cts/bulk", "import/cts/bulk")]
    CtsBulk,
    [DataSourceTypeInfo("DELTA", "cads/cts/daily", "import/cts/daily")]
    CtsDelta
}