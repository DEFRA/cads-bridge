using CadsBridge.Core.ApiClients;
using CadsBridge.Infrastructure.ApiClients.DTOs;
using CadsBridge.Infrastructure.ApiClients.DTOs.Requests;

namespace CadsBridge.Infrastructure.ApiClients.Contracts;

public interface IFileImportApiService
{
    Task<FileImportDto?> GetByFileNameIfExists(string objectKey, CancellationToken cancellationToken);
    Task<FileImportDto?> GetByFileName(string objectKey, CancellationToken cancellationToken);
    Task<FileImport> Create(string objectKey, string destinationPrefix, long totalRowsToProcess, CancellationToken cancellationToken);
    Task Update(long id, UpdateFileImportRequest request, CancellationToken cancellationToken);
    Task MarkStatus(long id, FileImportStatus status, CancellationToken cancellationToken);
    Task MarkFailed(long id, string reason, CancellationToken cancellationToken);
    Task MarkReset(long id, CancellationToken cancellationToken);
}