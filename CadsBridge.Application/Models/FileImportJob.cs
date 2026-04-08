namespace CadsBridge.Application.Models;

public record FileImportJob(
    string JobId,
    string SourceKey,
    string TargetKey,
    string Password,
    string Salt,
    int? SplitFileSizeInMBytes,
    int? SplitLinesPerFile
);