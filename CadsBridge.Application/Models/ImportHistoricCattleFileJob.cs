namespace CadsBridge.Application.Models;

public record ImportHistoricCattleFileJob(
    string JobId,
    string SourceKey,
    string TargetKey,
    string Password,
    string Salt,
    SplitType SplitType,
    int? SplitValue);