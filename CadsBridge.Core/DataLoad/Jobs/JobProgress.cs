namespace CadsBridge.Core.DataLoad.Jobs;

public record JobProgress(
    string JobId,
    int TotalFiles,
    int CompletedFiles,
    IReadOnlyCollection<JobItemProgress> Files
);