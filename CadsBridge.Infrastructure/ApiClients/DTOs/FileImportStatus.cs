namespace CadsBridge.Infrastructure.ApiClients.DTOs;

public enum FileImportStatus : short
{
    None = 0,
    Pending = 1,
    Importing = 2,
    Completed = 3,
    Failed = 4
}