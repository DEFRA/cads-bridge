using CadsBridge.Core.ApiClients;

namespace CadsBridge.Infrastructure.ApiClients.DTOs;

public record FileImportStatusDto
{
    public long Id { get; set; }
    public string FileName { get; set; } = string.Empty;
    public long TotalRowsToProcess { get; set; }
    public long RowsFound { get; set; }
    public FileImportStatus ImportStatus { get; set; }
    public FileProcessingStatus ProcessingStatus { get; set; }
}