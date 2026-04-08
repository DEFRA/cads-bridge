namespace CadsBridge.Application.Models;

public record FileSplitJob(
    string JobId,
    string Key,
    string TargetFolder,
    int? FileSizeInMBytes,
    int? LinesPerFile
);