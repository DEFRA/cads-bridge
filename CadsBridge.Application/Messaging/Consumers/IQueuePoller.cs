using CadsBridge.Application.Messaging.Clients;

namespace CadsBridge.Application.Messaging.Consumers;

public interface IQueuePoller<in T>
    where T : IQueueClient, new()
{
    string? QueueUrl { get; }

    Task StartAsync(CancellationToken token);
    Task StopAsync(CancellationToken token);
}
