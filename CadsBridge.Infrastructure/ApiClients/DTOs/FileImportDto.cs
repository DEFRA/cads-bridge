using CadsBridge.Core.ApiClients;

namespace CadsBridge.Infrastructure.ApiClients.DTOs;

public record FileImportDto
{
    public FileImportDto()
    { }

    public FileImportDto(int failedAttempts)
    {
        FailedAttempts = failedAttempts;
    }

    public long Id { get; set; }
    public string FileName { get; set; } = string.Empty;
    public string DestinationTableName { get; set; } = string.Empty;
    public long TotalRowsToProcess { get; set; }
    public long RowsFound { get; set; }
    public FileImportStatus ImportStatus { get; set; }
    public FileProcessingStatus ProcessingStatus { get; set; }
    public int FailedAttempts { get; }

    public FileImport ToFileImport()
    {
        return new FileImport
        {
            Id = Id,
            FileName = FileName,
            DestinationTableName = DestinationTableName,
            ImportStatus = ImportStatus,
            FailedAttempts = FailedAttempts
        };
    }
}