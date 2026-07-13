using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Observers;

namespace CadsBridge.Testing.Support.TestDoubles.Observers;

public class TestQueuePollerObserver<T> : IQueuePollerObserver<T>
{
    private readonly TaskCompletionSource<(string MessageId, T? Payload)> _handledTcs = new();
    private readonly TaskCompletionSource<(string MessageId, Exception Error)> _failedTcs = new();

    public Task<(string MessageId, T? Payload)> MessageHandled => _handledTcs.Task;
    public Task<(string MessageId, Exception Error)> MessageFailed => _failedTcs.Task;

    public void OnMessageHandled(string messageId, DateTime handledAt, T? payload, Message rawMessage)
    {
        _handledTcs.TrySetResult((messageId, payload));
    }

    public void OnMessageFailed(string messageId, DateTime failedAt, Exception exception, Message rawMessage)
    {
        _failedTcs.TrySetResult((messageId, exception));
    }
}