using Amazon.SQS;
using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Messages;
using CadsBridge.Application.Messaging.Models;
using CadsBridge.Application.Messaging.Observers;
using CadsBridge.Application.Messaging.Services;
using CadsBridge.Infrastructure.Messaging.Configuration;
using CadsBridge.Infrastructure.Messaging.Factories;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CadsBridge.Infrastructure.Messaging.Consumers;

public class CadsBridgeFifoQueuePoller(
    IServiceScopeFactory scopeFactory,
    IAmazonSQS sqs,
    MessageCommandRegistry registry,
    IOptionsMonitor<QueueConsumerOptions> options,
    CadsBridgeFifoQueueClient client,
    IQueueAdminService<CadsBridgeFifoQueueClient> queueAdminService,
    IQueuePollerObserver<MessageType> observer,
    ILogger<CadsBridgeFifoQueuePoller> logger)
        : BaseSqsQueuePoller<CadsBridgeFifoQueueClient>(scopeFactory, sqs, options, client, queueAdminService, observer, logger)
{
    protected override async Task<MessageType?> ProcessMessageAsync(
        UnwrappedMessage message,
        CancellationToken cancellationToken)
    {
        using var scope = ScopeFactory.CreateScope();
        var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
        var command = registry.CreateCommand(message);
        return await mediator.Send(command, cancellationToken);
    }
}