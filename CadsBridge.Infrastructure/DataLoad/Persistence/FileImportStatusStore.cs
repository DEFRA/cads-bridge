using System.Net;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.DataLoad.Persistence;

public class FileImportStatusStore : IFileImportStatusStore
{
    private readonly IFileImportStatusApiService _fileImportStatusApiService;
    private readonly ILogger<FileImportStatusStore> _logger;

    public FileImportStatusStore(IFileImportStatusApiService fileImportStatusApiService, ILogger<FileImportStatusStore> logger)
    {
        _fileImportStatusApiService = fileImportStatusApiService;
        _logger = logger;
    }

    public async Task<long> Initiate(string fileName, long totalRowsToProcess, CancellationToken cancellationToken = default)
    {
        try
        {
            var result = await _fileImportStatusApiService.Create(fileName, totalRowsToProcess, cancellationToken);
            return result;
        }
        catch (HttpRequestException ex)
        {
            if (ex.StatusCode != HttpStatusCode.Conflict)
            {
                throw;
            }
            if (_logger.IsEnabled(LogLevel.Error))
            {
                _logger.LogError(ex, "Failed to create file import status as filename already exists: {FileName}", fileName);
            }
            // TODO: Handle conflict
            return 0;
        }
    }

    public Task MarkInProgress(long fileImportStatusId, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task MarkSucceeded(long fileImportStatusId, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task MarkFailed(long fileImportStatusId, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }
}