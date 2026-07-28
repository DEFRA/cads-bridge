using System.Text.Json.Serialization;

namespace CadsBridge.Core.ApiClients;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum FileImportStatus : short
{
    None = 0,
    Pending = 1,
    Transferred = 2,
    Split = 3,
    Completed = 4,
    Failed = 5
}