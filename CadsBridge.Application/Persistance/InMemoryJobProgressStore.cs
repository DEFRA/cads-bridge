using CadsBridge.Application.Models;
using System.Collections.Concurrent;

namespace CadsBridge.Application.Persistance;

public record JobItemProgress(
    string Key,
    JobStatus Status,
    string? ErrorMessage
);

public record JobProgress(
    string JobId,
    int TotalFiles,
    int CompletedFiles,
    IReadOnlyCollection<JobItemProgress> Files
);

public abstract class InMemoryJobProgressStore
{
    private class MutableJob
    {
        public int TotalFiles { get; set; }
        public ConcurrentDictionary<string, JobItemProgress> Files { get; } = new();
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
        job.Files[key] = new JobItemProgress(key, JobStatus.InProgress, null);
    }

    public void MarkSucceeded(string jobId, string key)
    {
        if (!_jobs.TryGetValue(jobId, out var job)) return;
        job.Files[key] = new JobItemProgress(key, JobStatus.Succeeded, null);
    }

    public void MarkFailed(string jobId, string key, string error)
    {
        if (!_jobs.TryGetValue(jobId, out var job)) return;
        job.Files[key] = new JobItemProgress(key, JobStatus.Failed, error);
    }

    public JobProgress? GetJob(string jobId)
    {
        if (!_jobs.TryGetValue(jobId, out var job)) return null;

        var files = job.Files.Values.ToList();
        var completed = files.Count(f =>
            f.Status is JobStatus.Succeeded or JobStatus.Failed);

        return new JobProgress(
            jobId,
            job.TotalFiles,
            completed,
            files
        );
    }

    public IEnumerable<JobProgress> GetJobs()
    {
        return
            _jobs.Select(kvp =>
            {
                var jobId = kvp.Key;
                var job = kvp.Value;
                var files = job.Files.Values.ToList();
                var completed = files.Count(f =>
                    f.Status is JobStatus.Succeeded or JobStatus.Failed);
                return new JobProgress(
                    jobId,
                    job.TotalFiles,
                    completed,
                    files
                );
            }
        );
    }
}