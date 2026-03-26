using System.Text.Json;
using System.Text.Json.Serialization;

namespace CadsBridge.Infrastructure.Json;

public static class JsonDefaults
{
    public static JsonSerializerOptions DefaultOptions { get; set; } = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        Converters = { }
    };

    public static JsonSerializerOptions DefaultOptionsWithStringEnumConversion { get; set; } = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        Converters = { new JsonStringEnumConverter() }
    };

    public static JsonSerializerOptions DefaultOptionsWithIndented { get; set; } = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true
    };
}