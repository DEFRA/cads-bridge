namespace CadsBridge.Infrastructure.ApiClients.DTOs;

public enum FileProcessingStatus : short
{
    None = 0,
    Pending = 1,
    Processing = 2,
    Completed = 3,
    Failed = 4
}