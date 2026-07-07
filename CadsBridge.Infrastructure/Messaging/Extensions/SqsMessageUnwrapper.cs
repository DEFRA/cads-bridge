using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Models;

namespace CadsBridge.Infrastructure.Messaging.Extensions;

public static class SqsMessageUnwrapper
{
    public static UnwrappedMessage Unwrap(this Message message)
    {
        ArgumentNullException.ThrowIfNull(message);

        return UnwrapFromRawMessage(message);
    }

    private static UnwrappedMessage UnwrapFromRawMessage(Message message)
    {
        var subject = message.GetMessageAttributeValue<string>("Subject") ?? "Default";
        var correlationId = message.GetMessageAttributeValue<string>("CorrelationId") ?? string.Empty;

        return new UnwrappedMessage
        {
            MessageId = message.MessageId,
            CorrelationId = correlationId,
            Subject = subject.ReplaceSuffix(),
            Payload = message.Body ?? string.Empty,
            Attributes = message.MessageAttributes?.ToDictionary(
                kvp => kvp.Key,
                kvp => kvp.Value?.StringValue ?? string.Empty)
        };
    }
}