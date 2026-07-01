using CadsBridge.Infrastructure.ApiClients.DTOs;

namespace CadsBridge.Infrastructure.ApiClients.Contracts;

public interface IFileImportStatusApiService
{
    Task<FileImportStatusDto?> GetByS3Key(string s3Key, CancellationToken cancellationToken);
    Task<FileImportStatusDto?> GetById(Guid id, CancellationToken cancellationToken);
    Task<Guid> Create(string s3Key, long recordCount, CancellationToken cancellationToken);
}