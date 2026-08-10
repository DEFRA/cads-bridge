namespace CadsBridge.Application.DataLoad.Jobs;

public record DataSeedFileLoadJob(
    string JobId,
    string FileName,
    string TargetKey,
    string? CorrelationId = null);