namespace CadsBridge.Application.Models;

public record FileImportRequest(
    string JobId,
    string sourceKey,
    string targetKey,
    string Password,
    string Salt,
    int? SplitFileSizeInMBytes,
    int? SplitLinesPerFile
);