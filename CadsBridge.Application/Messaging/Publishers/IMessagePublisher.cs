namespace CadsBridge.Application.Messaging.Publishers;

public interface IMessagePublisher<in T> where T : class, new()
{
    string? QueueUrl { get; }

    Task PublishAsync<TMessage>(TMessage? message, CancellationToken cancellationToken = default) where TMessage : class;
}