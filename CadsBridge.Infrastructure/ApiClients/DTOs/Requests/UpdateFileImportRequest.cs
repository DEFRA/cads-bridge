namespace CadsBridge.Infrastructure.ApiClients.DTOs.Requests;

public class UpdateFileImportRequest
{
    public long TotalRowsToProcess { get; set; }
    public long RowsFound { get; set; }
    public FileImportStatus ImportStatus { get; set; }
}