using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.DTOs;
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

    public async Task MarkInProgress(long fileImportStatusId, CancellationToken cancellationToken = default)
    {
        await _fileImportStatusApiService.MarkStatus(fileImportStatusId, FileImportStatus.Importing, cancellationToken);
    }

    public async Task MarkSucceeded(long fileImportStatusId, CancellationToken cancellationToken = default)
    {
        await _fileImportStatusApiService.MarkStatus(fileImportStatusId, FileImportStatus.Completed, cancellationToken);
    }

    public async Task MarkFailed(long fileImportStatusId, CancellationToken cancellationToken = default)
    {
        await _fileImportStatusApiService.MarkStatus(fileImportStatusId, FileImportStatus.Failed, cancellationToken);
    }
}