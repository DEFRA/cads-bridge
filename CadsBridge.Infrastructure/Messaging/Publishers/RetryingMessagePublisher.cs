using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Models;
using CadsBridge.Application.Messaging.Publishers;
using Polly;

namespace CadsBridge.Infrastructure.Messaging.Publishers;

public class RetryingMessagePublisher<TClient>(
    IMessagePublisher<TClient> inner,
    ResiliencePipeline pipeline)
    : IMessagePublisher<TClient> where TClient : IQueueClient, new()
{
    public string QueueUrl => inner.QueueUrl!;

    public Task PublishAsync<TMessage>(TMessage? message, FifoMessageMetadata metadata, CancellationToken cancellationToken = default)
        where TMessage : class
        => pipeline.ExecuteAsync(ct => new ValueTask<Task>(inner.PublishAsync(message, metadata, ct)),
            cancellationToken).AsTask().Unwrap();
}