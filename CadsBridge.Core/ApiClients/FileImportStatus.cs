namespace CadsBridge.Core.ApiClients;

public enum FileImportStatus : short
{
    None = 0,
    Pending = 1,
    Importing = 2,
    Completed = 3,
    Failed = 4
}