namespace CadsBridge.Application.Models;

public record HistoricDataFileSplitJob(
    string JobId,
    string Key,
    string TargetFolder,
    SplitType SplitType,
    int? SplitValue
);