using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Clients;

namespace CadsBridge.Application.Messaging.Services;

public interface IQueueAdminService<in T>
    where T : IQueueClient, new()
{
    Task<bool> MoveToDeadLetterQueueAsync(
        Message message,
        string queueUrl,
        string? dlqQueueUrl,
        Exception ex,
        CancellationToken cancellationToken);
}