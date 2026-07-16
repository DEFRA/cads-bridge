using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Core.ApiClients;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.DTOs.Requests;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.DataLoad.Persistence;

public class FileImportStore(IFileImportApiService fileImportStatusApiService, ILogger<FileImportStore> logger) : IFileImportStore
{
    private readonly IFileImportApiService _fileImportStatusApiService = fileImportStatusApiService;
    private readonly ILogger<FileImportStore> _logger = logger;

    public async Task<long> CreateAsync(string fileName, long totalRowsToProcess = 0, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _fileImportStatusApiService.Create(fileName, totalRowsToProcess, cancellationToken);
        }
        catch (ConflictException ex)
        {
            if (_logger.IsEnabled(LogLevel.Warning))
            {
                _logger.LogWarning(ex, "File import already exists, resetting existing record: {FileName}", fileName);
            }
            return await MarkFileReset(fileName, cancellationToken);
        }
    }

    public Task UpdateAsync(long fileImportId, FileImportStatus status, long totalRowsToProcess, long rowsFound = 0, CancellationToken cancellationToken = default)
    {
        var request = new UpdateFileImportRequest
        {
            Status = status,
            TotalRowsToProcess = totalRowsToProcess,
            RowsFound = rowsFound
        };

        return _fileImportStatusApiService.Update(fileImportId, request, cancellationToken);
    }

    private async Task<long> MarkFileReset(string fileName, CancellationToken cancellationToken = default)
    {
        var fileImportStatus = await _fileImportStatusApiService.GetByFileName(fileName, cancellationToken);
        if (fileImportStatus is null)
        {
            throw new NotFoundException(
                $"File import status for '{fileName}' was not found when attempting to reset it after a conflict.");
        }

        await _fileImportStatusApiService.MarkReset(fileImportStatus.Id, cancellationToken);
        return fileImportStatus.Id;
    }

    public async Task MarkInProgressAsync(long fileImportId, CancellationToken cancellationToken = default)
    {
        await _fileImportStatusApiService.MarkStatus(fileImportId, FileImportStatus.Importing, cancellationToken);
    }

    public async Task MarkCompletedAsync(long fileImportId, CancellationToken cancellationToken = default)
    {
        await _fileImportStatusApiService.MarkStatus(fileImportId, FileImportStatus.Completed, cancellationToken);
    }

    public async Task MarkFailedAsync(long fileImportId, CancellationToken cancellationToken = default)
    {
        await _fileImportStatusApiService.MarkStatus(fileImportId, FileImportStatus.Failed, cancellationToken);
    }
}