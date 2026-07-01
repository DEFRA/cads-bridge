namespace CadsBridge.Infrastructure.ApiClients.DTOs;

public record FileImportStatusDto
{
    public Guid Id { get; set; }
    public string S3Key { get; set; } = string.Empty;
    public int TotalRowsToProcess { get; set; }
    public int RowsProcessed { get; set; }
    public ImportStatus ImportStatus { get; set; }
    public DateTime? LastUpdated { get; set; }
}