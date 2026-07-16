using CadsBridge.Application.Messaging.Messages;
using System.Text.Json.Serialization;

namespace CadsBridge.Infrastructure.Messaging.Serializers;

[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    Converters = []
)]
[JsonSerializable(typeof(CsvDataFileImportMessage))]
public partial class MessageIdentifierSerializerContext : JsonSerializerContext
{
}