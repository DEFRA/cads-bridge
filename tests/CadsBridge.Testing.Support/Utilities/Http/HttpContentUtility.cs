using System.Text;
using System.Text.Json;
using CadsBridge.Infrastructure.Json;

namespace CadsBridge.Testing.Support.Utilities.Http;

public static class HttpContentUtility
{
    public static StringContent CreateApplicationJsonAsStringContent<T>(T data)
    {
        var stringContent = new StringContent(
            content: JsonSerializer.Serialize(data, JsonDefaults.DefaultOptions),
            encoding: Encoding.UTF8,
            mediaType: "application/json");

        return stringContent;
    }

    public static StringContent CreateApplicationJsonAsStringContent(string data)
    {
        var stringContent = new StringContent(
            content: data,
            encoding: Encoding.UTF8,
            mediaType: "application/json");

        return stringContent;
    }
}