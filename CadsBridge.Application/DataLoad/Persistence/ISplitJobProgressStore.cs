using CadsBridge.Core.DataLoad.Jobs;

namespace CadsBridge.Application.DataLoad.Persistence;

public interface ISplitJobProgressStore
{
    void InitJob(string jobId, int totalFiles);
    void MarkInProgress(string jobId, string key);
    void MarkSucceeded(string jobId, string key);
    void MarkFailed(string jobId, string key, string error);
    JobProgress? GetJob(string jobId);
    IEnumerable<JobProgress> GetJobs();
}