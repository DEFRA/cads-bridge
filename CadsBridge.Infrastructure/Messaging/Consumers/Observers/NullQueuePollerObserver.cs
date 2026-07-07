using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Observers;

namespace CadsBridge.Infrastructure.Messaging.Consumers.Observers;

public class NullQueuePollerObserver<T> : IQueuePollerObserver<T>
{
    public void OnMessageHandled(string messageId, DateTime handledAt, T payload, Message rawMessage) { }
    public void OnMessageFailed(string messageId, DateTime failedAt, Exception exception, Message rawMessage) { }
}