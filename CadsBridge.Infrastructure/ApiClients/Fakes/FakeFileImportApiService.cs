using CadsBridge.Core.ApiClients;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.DTOs;
using CadsBridge.Infrastructure.ApiClients.DTOs.Requests;

namespace CadsBridge.Infrastructure.ApiClients.Fakes;

public class FakeFileImportApiService : IFileImportApiService
{
    private readonly Random _random = new();

    public Task<FileImportDto?> GetByFileNameIfExists(string objectKey, CancellationToken cancellationToken)
    {
        return objectKey == "not-found.csv" ? Task.FromResult<FileImportDto?>(null) : GetByFileName(objectKey, cancellationToken);
    }

    public Task<FileImportDto?> GetByFileName(string objectKey, CancellationToken cancellationToken)
    {
        var response = new FileImportDto
        {
            Id = _random.Next(1, 99),
            FileName = objectKey,
            ImportStatus = FileImportStatus.Pending,
            ProcessingStatus = FileProcessingStatus.Pending,
            TotalRowsToProcess = 0,
            RowsFound = 0
        };
        return Task.FromResult<FileImportDto?>(response);
    }

    public Task<long> Create(string objectKey, long totalRowsToProcess, CancellationToken cancellationToken)
    {
        return Task.FromResult<long>(_random.Next(1, 99));
    }

    public Task Update(long id, UpdateFileImportRequest request, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task MarkStatus(long id, FileImportStatus status, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task MarkFailed(long id, string reason, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task MarkReset(long id, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}