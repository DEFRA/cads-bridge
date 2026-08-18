using CadsBridge.Core.DataLoad;

namespace CadsBridge.Infrastructure.DataLoad.Configuration;

public class DataLoadConfiguration
{
    public string Salt { get; set; } = string.Empty;
    public SplitType SplitType { get; set; } = SplitType.ByLines;
    public int? SplitValue { get; set; } = 10000;
    public int MaxRetryAttempts { get; set; } = 3;
    public int RetryDelayBase { get; set; } = 500;
    public int MaxParallelDownloads { get; set; } = 4;
    public int MarkFailedTimeoutSeconds { get; set; } = 10;
}