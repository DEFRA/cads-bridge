using Amazon.SQS.Model;

namespace CadsBridge.Infrastructure.Messaging.Extensions;

public static class MessageAttributeExtensions
{
    private const string MESSAGE_SUFFIX = "Message";

    public static T? GetMessageAttributeValue<T>(this Message message, string key)
    {
        if (message.MessageAttributes is null || !message.MessageAttributes.TryGetValue(key, out var attribute))
            return default;

        return ParseAttributeValue<T>(attribute.DataType, attribute.StringValue);
    }

    private static T? ParseAttributeValue<T>(string type, string? raw)
    {
        if (raw is null) return default;

        if (typeof(T) == typeof(string) && type == "String")
            return (T)(object)raw;

        if (typeof(T) == typeof(int) && type == "Number" && int.TryParse(raw, out var intVal))
            return (T)(object)intVal;

        if (typeof(T) == typeof(double) && type == "Number" && double.TryParse(raw, out var doubleVal))
            return (T)(object)doubleVal;

        return default;
    }

    public static string ReplaceSuffix(this string? messageName)
    {
        return messageName?.EndsWith(MESSAGE_SUFFIX) == true
            ? messageName[..^MESSAGE_SUFFIX.Length]
            : messageName ?? string.Empty;
    }
}