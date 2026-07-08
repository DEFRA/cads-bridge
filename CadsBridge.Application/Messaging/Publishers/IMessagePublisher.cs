using CadsBridge.Application.Messaging.Models;

namespace CadsBridge.Application.Messaging.Publishers;

public interface IMessagePublisher<in T> where T : class, new()
{
    string? QueueUrl { get; }

    Task PublishAsync<TMessage>(TMessage? message, FifoMessageMetadata metadata, CancellationToken cancellationToken = default) where TMessage : class;
}