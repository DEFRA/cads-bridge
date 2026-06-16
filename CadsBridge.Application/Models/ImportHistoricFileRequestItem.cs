namespace CadsBridge.Application.Models;

public record ImportHistoricFileRequestItem(
    string JobId,
    string sourceKey,
    string targetKey,
    string Password,
    string Salt,
    SplitType SplitType,
    int? SplitValue
);