namespace CadsBridge.Application.Models;

public record SplitRequestItem(
    string JobId,
    string Key,
    string TargetFolder,
    SplitType SplitType,
    int? SplitValue
);