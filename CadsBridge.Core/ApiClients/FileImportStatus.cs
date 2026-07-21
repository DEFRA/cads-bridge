namespace CadsBridge.Core.ApiClients;

public enum FileImportStatus : short
{
    None = 0,
    Pending = 1,
    Transferred = 2,
    Split = 3,
    Completed = 4,
    Failed = 5
}