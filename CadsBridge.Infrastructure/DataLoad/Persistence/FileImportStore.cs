using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Core.ApiClients;
using CadsBridge.Core.Domain.BusinessRules;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.DTOs.Requests;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.DataLoad.Persistence;

public class FileImportStore(IFileImportApiService fileImportStatusApiService, ILogger<FileImportStore> logger) : IFileImportStore
{
    public async Task<long> CreateAsync(string fileName, string destinationPrefix, long totalRowsToProcess = 0, CancellationToken cancellationToken = default)
    {
        try
        {
            var fileImport = await fileImportStatusApiService.Create(fileName, destinationPrefix, totalRowsToProcess, cancellationToken);
            BusinessRuleChecker.CheckRule(new DestinationTableNameIsValid(fileImport));
            return fileImport.Id;
        }
        catch (ConflictException)
        {
            return await MarkFileReset(fileName, cancellationToken);
        }
    }

    public async Task UpdateAsync(long fileImportId, FileImportStatus status, long totalRowsToProcess, long rowsFound = 0, CancellationToken cancellationToken = default)
    {
        var request = new UpdateFileImportRequest
        {
            ImportStatus = status,
            TotalRowsToProcess = totalRowsToProcess,
            RowsFound = rowsFound
        };

        await fileImportStatusApiService.Update(fileImportId, request, cancellationToken);
    }

    private async Task<long> MarkFileReset(string fileName, CancellationToken cancellationToken = default)
    {
        try
        {
            var fileImportStatus = await fileImportStatusApiService.GetByFileName(fileName, cancellationToken)
                ?? throw new NotFoundException(
                    $"File import status for '{fileName}' was not found when attempting to reset it.");

            if (fileImportStatus.ImportStatus == FileImportStatus.Completed)
            {
                throw new InvalidOperationException(
                    $"File import status for '{fileName}' is already marked as completed and cannot be reset.");
            }
            if (fileImportStatus.ImportStatus == FileImportStatus.Failed && fileImportStatus.FailedAttempts >= 3)
            {
                throw new InvalidOperationException(
                    $"File import status for '{fileName}' has failed too many times and cannot be reset.");
            }

            await fileImportStatusApiService.MarkReset(fileImportStatus.Id, cancellationToken);
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("File import already exists and has been reset: {FileName}", fileName);
            }

            return fileImportStatus.Id;
        }
        catch (NotFoundException ex)
        {
            if (logger.IsEnabled(LogLevel.Warning))
            {
                logger.LogWarning(ex, "File import status for {FileName} was not found when attempting to reset it.", fileName);
            }
            throw;
        }
    }

    public async Task MarkFailedAsync(long fileImportId, string? reason = null, CancellationToken cancellationToken = default)
    {
        reason ??= "import failed";
        await fileImportStatusApiService.MarkFailed(fileImportId, reason, cancellationToken);
    }
}