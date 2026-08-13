namespace CadsBridge.Core.ApiClients;

public class FileImport
{
    public long Id { get; set; }
    public string FileName { get; set; } = string.Empty;
    public string DestinationTableName { get; set; } = string.Empty;
    public FileImportStatus ImportStatus { get; set; }
    public int FailedAttempts { get; set; }
}