using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Observers;
using System.Diagnostics.CodeAnalysis;

namespace CadsBridge.Infrastructure.Messaging.Consumers.Observers;

[ExcludeFromCodeCoverage]
public class NullQueuePollerObserver<T> : IQueuePollerObserver<T>
{
    public void OnMessageHandled(string messageId, DateTime handledAt, T? payload, Message rawMessage) { }
    public void OnMessageFailed(string messageId, DateTime failedAt, Exception exception, Message rawMessage) { }
}