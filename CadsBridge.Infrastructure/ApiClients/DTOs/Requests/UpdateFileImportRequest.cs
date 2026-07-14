using CadsBridge.Core.ApiClients;

namespace CadsBridge.Infrastructure.ApiClients.DTOs.Requests;

public class UpdateFileImportRequest
{
    public FileImportStatus Status { get; set; }
    public long TotalRowsToProcess { get; set; }
    public long RowsProcessed { get; set; }
}