namespace CadsBridge.Application.DataLoad.Persistence;

public interface IFileImportStatusStore
{
    Task<long> Initiate(string fileName, long totalRowsToProcess, CancellationToken cancellationToken = default);
    Task MarkInProgress(long fileImportStatusId, CancellationToken cancellationToken = default);
    Task MarkSucceeded(long fileImportStatusId, CancellationToken cancellationToken = default);
    Task MarkFailed(long fileImportStatusId, CancellationToken cancellationToken = default);
}