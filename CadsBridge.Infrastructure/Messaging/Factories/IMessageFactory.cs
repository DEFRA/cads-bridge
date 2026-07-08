using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Models;

namespace CadsBridge.Infrastructure.Messaging.Factories;

public interface IMessageFactory
{
    SendMessageRequest CreateFifoSqsMessage<TBody>(
        string queueUrl,
        TBody body,
        FifoMessageMetadata metadata,
        string? subject = null);
}