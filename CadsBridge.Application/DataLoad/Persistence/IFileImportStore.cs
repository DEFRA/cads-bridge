using CadsBridge.Core.ApiClients;

namespace CadsBridge.Application.DataLoad.Persistence;

public interface IFileImportStore
{
    Task<long> CreateAsync(string fileName, long totalRowsToProcess = 0, CancellationToken cancellationToken = default);

    Task UpdateAsync(long fileImportId, FileImportStatus status, long totalRowsToProcess, long rowsFound = 0, CancellationToken cancellationToken = default);

    Task MarkTransferredAsync(long fileImportId, CancellationToken cancellationToken = default);

    Task MarkSplitAsync(long fileImportId, CancellationToken cancellationToken = default);

    Task MarkCompletedAsync(long fileImportId, CancellationToken cancellationToken = default);

    Task MarkFailedAsync(long fileImportId, string? reason = null, CancellationToken cancellationToken = default);
}