using CadsBridge.Core.ApiClients;

namespace CadsBridge.Application.DataLoad.Persistence;

public interface IFileImportStore
{
    Task<long> Initiate(string fileName, long totalRowsToProcess, CancellationToken cancellationToken = default);

    Task Update(long fileImportId, FileImportStatus status, long totalRowsToProcess, long rowsProcessed, CancellationToken cancellationToken = default);

    Task MarkInProgress(long fileImportId, CancellationToken cancellationToken = default);

    Task MarkSucceeded(long fileImportId, CancellationToken cancellationToken = default);

    Task MarkFailed(long fileImportId, CancellationToken cancellationToken = default);
}