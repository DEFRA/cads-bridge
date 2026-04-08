namespace CadsBridge.Application.Models;

public record FileSplitRequest(
    string JobId,
    string Key,
    string TargetFolder,
    int? FileSizeInMBytes,
    int? LinesPerFile
);