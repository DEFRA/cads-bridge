namespace CadsBridge.Infrastructure.ApiClients.DTOs.Requests;

public class CreateFileImportRequest
{
    public string FileName { get; set; } = default!;

    public string DestinationPrefix { get; set; } = default!;

    public long TotalRowsToProcess { get; set; }
}