namespace CadsBridge.Application.Models;

public record SplitRequestItem(
    string Key,
    string TargetFolder,
    SplitType SplitType,
    int? SplitValue
);