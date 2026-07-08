using Amazon.SQS.Model;

namespace CadsBridge.Application.Messaging.Services;

public interface IQueueAdminService
{
    Task<bool> MoveToDeadLetterQueueAsync(
        Message message,
        string queueUrl,
        Exception ex,
        CancellationToken cancellationToken);
}
