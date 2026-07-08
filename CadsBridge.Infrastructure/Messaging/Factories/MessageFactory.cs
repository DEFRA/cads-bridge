using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Models;
using CadsBridge.Core.Correlation;
using CadsBridge.Infrastructure.Json;
using CadsBridge.Infrastructure.Messaging.Extensions;
using System.Text.Json;

namespace CadsBridge.Infrastructure.Messaging.Factories;

public class MessageFactory : IMessageFactory
{
    private const string EventTimeUtc = "EventTimeUtc";
    private const string StringDataType = "String";

    public SendMessageRequest CreateFifoSqsMessage<TBody>(
        string queueUrl,
        TBody body,
        FifoMessageMetadata metadata,
        string? subject = null)
    {
        var messageType = typeof(TBody).Name;
        var payload = SerializeToJson(body);
        var resolvedSubject = subject ?? messageType;

        var attributes = BuildSqsAttributes(resolvedSubject, metadata.AdditionalAttributes);

        if (attributes.TryGetValue("CorrelationId", out var existing))
        {
            existing.StringValue = metadata.CorrelationId;
        }
        else
        {
            attributes["CorrelationId"] = new MessageAttributeValue
            {
                DataType = StringDataType,
                StringValue = metadata.CorrelationId
            };
        }

        return new SendMessageRequest
        {
            QueueUrl = queueUrl,
            MessageBody = payload,
            MessageGroupId = metadata.MessageGroupId,
            MessageDeduplicationId = metadata.MessageDeduplicationId,
            MessageAttributes = attributes
        };
    }

    private static Dictionary<string, MessageAttributeValue> BuildSqsAttributes(
        string subject,
        IReadOnlyDictionary<string, string>? additionalUserProperties)
    {
        var attributes = new Dictionary<string, MessageAttributeValue>
        {
            [EventTimeUtc] = new MessageAttributeValue
            {
                DataType = StringDataType,
                StringValue = DateTime.UtcNow.ToString("O")
            },
            ["Subject"] = new MessageAttributeValue
            {
                DataType = StringDataType,
                StringValue = subject.ReplaceSuffix()
            },
            ["CorrelationId"] = new MessageAttributeValue
            {
                DataType = StringDataType,
                StringValue = CorrelationIdContext.Value ?? Guid.NewGuid().ToString()
            }
        };

        if (additionalUserProperties == null)
            return attributes;

        foreach (var (key, value) in additionalUserProperties)
        {
            attributes[key] = new MessageAttributeValue
            {
                DataType = StringDataType,
                StringValue = value
            };
        }

        return attributes;
    }

    private static string SerializeToJson<TBody>(TBody value)
    {
        return typeof(TBody) switch
        {
            // Add specific 'Source Generations' here for message types
            _ => JsonSerializer.Serialize(value, JsonDefaults.DefaultOptionsWithStringEnumConversion)
        };
    }
}