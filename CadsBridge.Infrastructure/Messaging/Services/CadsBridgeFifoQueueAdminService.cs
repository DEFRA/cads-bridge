using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Services;

namespace CadsBridge.Infrastructure.Messaging.Services;

public class CadsBridgeFifoQueueAdminService : IQueueAdminService
{
    public Task<bool> MoveToDeadLetterQueueAsync(
        Message message,
        string queueUrl,
        Exception ex,
        CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
