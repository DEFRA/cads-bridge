namespace CadsBridge.Application.Models;

public record FileSplitJob(
    string JobId,
    string Key,
    string TargetFolder,
    SplitType SplitType,
    int? SplitValue
);