using CadsBridge.Infrastructure.ApiClients.DTOs;

namespace CadsBridge.Infrastructure.ApiClients.Contracts;

public interface IFileImportStatusApiService
{
    Task<FileImportStatusDto?> GetByFileName(string fileName, CancellationToken cancellationToken);
    Task<FileImportStatusDto?> GetById(Guid id, CancellationToken cancellationToken);
    Task<long> Create(string s3Key, long recordCount, CancellationToken cancellationToken);
}