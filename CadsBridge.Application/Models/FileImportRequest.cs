namespace CadsBridge.Application.Models;

public enum ImportStatus
{
    Pending,
    InProgress,
    Succeeded,
    Failed
}

public record FileImportRequest(
    string JobId,
    string sourceKey,
    string targetKey,
    string Password,
    string Salt
);