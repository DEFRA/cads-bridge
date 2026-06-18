namespace CadsBridge.Core.DataLoad.Jobs;

public record JobItemProgress(
    string Key,
    JobStatus Status,
    string? ErrorMessage
);