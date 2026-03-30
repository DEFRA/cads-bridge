using CadsBridge.Application.Models;
using System.Collections.Concurrent;

namespace CadsBridge.Application.Persistance;

public interface IImportProgressStore
{
    void InitJob(string jobId, int totalFiles);
    void MarkInProgress(string jobId, string key);
    void MarkSucceeded(string jobId, string key);
    void MarkFailed(string jobId, string key, string error);
    ImportJobProgress? GetJob(string jobId);
}

public record ImportFileProgress(
    string Key,
    ImportStatus Status,
    string? ErrorMessage
);

public record ImportJobProgress(
    string JobId,
    int TotalFiles,
    int CompletedFiles,
    IReadOnlyCollection<ImportFileProgress> Files
);

public class InMemoryImportProgressStore : IImportProgressStore
{
    private class MutableJob
    {
        public int TotalFiles { get; set; }
        public ConcurrentDictionary<string, ImportFileProgress> Files { get; } = new();
    }

    private readonly ConcurrentDictionary<string, MutableJob> _jobs = new();

    public void InitJob(string jobId, int totalFiles)
    {
        var job = new MutableJob { TotalFiles = totalFiles };
        _jobs[jobId] = job;
    }

    public void MarkInProgress(string jobId, string key)
    {
        if (!_jobs.TryGetValue(jobId, out var job)) return;
        job.Files[key] = new ImportFileProgress(key, ImportStatus.InProgress, null);
    }

    public void MarkSucceeded(string jobId, string key)
    {
        if (!_jobs.TryGetValue(jobId, out var job)) return;
        job.Files[key] = new ImportFileProgress(key, ImportStatus.Succeeded, null);
    }

    public void MarkFailed(string jobId, string key, string error)
    {
        if (!_jobs.TryGetValue(jobId, out var job)) return;
        job.Files[key] = new ImportFileProgress(key, ImportStatus.Failed, error);
    }

    public ImportJobProgress? GetJob(string jobId)
    {
        if (!_jobs.TryGetValue(jobId, out var job)) return null;

        var files = job.Files.Values.ToList();
        var completed = files.Count(f =>
            f.Status is ImportStatus.Succeeded or ImportStatus.Failed);

        return new ImportJobProgress(
            jobId,
            job.TotalFiles,
            completed,
            files
        );
    }
}