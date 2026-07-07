using Amazon.SQS.Model;

namespace CadsBridge.Infrastructure.Messaging.Factories;

public interface IMessageFactory
{
    SendMessageRequest CreateFifoSqsMessage<TBody>(
        string queueUrl,
        TBody body,
        FifoMessageMetadata metadata,
        string? subject = null);
}