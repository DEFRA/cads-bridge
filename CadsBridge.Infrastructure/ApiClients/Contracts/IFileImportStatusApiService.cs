using CadsBridge.Infrastructure.ApiClients.DTOs;
using CadsBridge.Infrastructure.ApiClients.DTOs.Requests;

namespace CadsBridge.Infrastructure.ApiClients.Contracts;

public interface IFileImportStatusApiService
{
    Task<FileImportStatusDto?> GetByFileName(string objectKey, CancellationToken cancellationToken);
    Task<long> Create(string objectKey, long totalRowsToProcess, CancellationToken cancellationToken);
    Task Update(long id, UpdateFileImportRequest request, CancellationToken cancellationToken);
    Task MarkStatus(long id, FileImportStatus status, CancellationToken cancellationToken);
    Task MarkReset(long id, CancellationToken cancellationToken);
}