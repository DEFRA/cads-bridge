using CadsBridge.Core.DataLoad.Jobs;

namespace CadsBridge.Infrastructure.DataLoad.Configuration;

public class DataLoadConfiguration
{
    public string Salt { get; set; } = string.Empty;
    public SplitType SplitType { get; set; } = SplitType.ByLines;
    public int? SplitValue { get; set; } = 10000;
}