namespace CadsBridge.Application.Models;

public record HistoricDataFileSplitRequestItem(
    string Key,
    string TargetFolder,
    SplitType SplitType,
    int? SplitValue
);