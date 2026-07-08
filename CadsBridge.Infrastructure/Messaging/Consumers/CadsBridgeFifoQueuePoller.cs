using Amazon.SQS;
using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Consumers;
using CadsBridge.Application.Messaging.Messages;
using CadsBridge.Application.Messaging.Observers;
using CadsBridge.Application.Messaging.Services;
using CadsBridge.Core.Correlation;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Messaging.Configuration;
using CadsBridge.Infrastructure.Messaging.Extensions;
using CadsBridge.Infrastructure.Messaging.Factories;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CadsBridge.Infrastructure.Messaging.Consumers;

public class CadsBridgeFifoQueuePoller(
    IServiceScopeFactory scopeFactory,
    IAmazonSQS amazonSQS,
    MessageCommandRegistry messageCommandRegistry,
    IOptionsMonitor<QueueConsumerOptions> options,
    CadsBridgeFifoQueueClient client,
    IQueueAdminService queueAdminService,
    IQueuePollerObserver<MessageType> observer,
    ILogger<CadsBridgeFifoQueuePoller> logger)
    : IQueuePoller<CadsBridgeFifoQueueClient>, IAsyncDisposable
{
    private Task? _pollingTask;
    private CancellationTokenSource _cts = new();

    public string QueueUrl => options.Get(client.ClientName).QueueUrl;
    public int MaxNumberOfMessages => options.Get(client.ClientName).MaxNumberOfMessages;
    public int WaitTimeSeconds => options.Get(client.ClientName).WaitTimeSeconds;

    public Task StartAsync(CancellationToken token)
    {
        logger.LogInformation("QueuePoller start requested.");

        _cts = CancellationTokenSource.CreateLinkedTokenSource(token);

        _pollingTask = Task.Run(() => PollMessagesAsync(_cts.Token), token);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken token)
    {
        logger.LogInformation("QueuePoller stop requested.");

        await _cts.CancelAsync();

        if (_pollingTask is { IsCompletedSuccessfully: false })
        {
            try
            {
                await _pollingTask;
            }
            catch (TaskCanceledException)
            {
                // Expected during cancellation
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_cts is not null)
        {
            await _cts.CancelAsync();
            _cts.Dispose();
        }

        if (_pollingTask is { IsCompleted: false })
        {
            try
            {
                await _pollingTask;
            }
            catch (TaskCanceledException)
            {
                // Swallow expected task cancellation during disposal
            }
        }

        GC.SuppressFinalize(this);
    }

    private async Task PollMessagesAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Connecting to queue: {QueueUrl}", QueueUrl);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var response = await amazonSQS.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = QueueUrl,
                    MaxNumberOfMessages = MaxNumberOfMessages,
                    WaitTimeSeconds = WaitTimeSeconds,
                    MessageAttributeNames = ["All"],
                    MessageSystemAttributeNames = ["All"]
                }, cancellationToken);

                var messages = response?.Messages;

                if (messages == null || messages.Count == 0) continue;

                logger.LogTrace("Completed receive for queue: {QueueUrl}, Number of messages: {count}",
                    QueueUrl, messages.Count);

                foreach (var message in messages)
                {
                    await HandleMessageAsync(message, QueueUrl, cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation("Poll operation cancelled for queue {QueueUrl}", QueueUrl);
            }
            catch (Exception ex)
            {
                logger.LogError("Unable to connect to queue: {QueueUrl} - Exception: {ex}",
                    QueueUrl, ex);

                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }

            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }
    }

    private async Task HandleMessageAsync(Message message, string queueUrl, CancellationToken cancellationToken)
    {
        try
        {
            var unwrappedMessage = message.Unwrap();
            CorrelationIdContext.Value = string.IsNullOrWhiteSpace(unwrappedMessage.CorrelationId)
                ? Guid.NewGuid().ToString()
                : unwrappedMessage.CorrelationId;

            logger.LogDebug("HandleMessageAsync using CorrelationId: {CorrelationId}", CorrelationIdContext.Value);

            var command = messageCommandRegistry.CreateCommand(unwrappedMessage);

            using var scope = scopeFactory.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
            var result = await mediator.Send(command, cancellationToken);

            await amazonSQS.DeleteMessageAsync(queueUrl, message.ReceiptHandle, cancellationToken);

            logger.LogInformation("Handled message with CorrelationId: {CorrelationId}", CorrelationIdContext.Value);

            observer?.OnMessageHandled(message.MessageId, DateTime.UtcNow, result, message);
        }
        catch (RetryableException ex)
        {
            HandleRetryableException(message, ex);
        }
        catch (NonRetryableException ex)
        {
            await HandleNonRetryableException(message, queueUrl, ex, cancellationToken);
        }
        catch (Exception ex)
        {
            await HandleUnexpectedException(message, queueUrl, ex, cancellationToken);
        }
    }

    private void HandleRetryableException(Message message, RetryableException ex)
    {
        var receiveCount = GetReceiveCount(message);

        logger.LogWarning("RetryableException in Queue: {Queue}, CorrelationId: {CorrelationId}, MessageId: {MessageId}, ReceiveCount: {ReceiveCount}, Exception: {Exception}",
                QueueUrl, CorrelationIdContext.Value, message.MessageId, receiveCount, ex);

        observer?.OnMessageFailed(message.MessageId, DateTime.UtcNow, ex, message);
    }

    private async Task HandleNonRetryableException(Message message, string queueUrl, NonRetryableException ex, CancellationToken cancellationToken)
    {
        logger.LogError("NonRetryableException in Queue: {Queue}, CorrelationId: {CorrelationId}, MessageId: {MessageId}, Exception: {Exception}",
            QueueUrl, CorrelationIdContext.Value, message.MessageId, ex);

        await MoveToDlqAndNotifyObserver(message, queueUrl, ex, cancellationToken);
    }

    private async Task HandleUnexpectedException(Message message, string queueUrl, Exception ex, CancellationToken cancellationToken)
    {
        logger.LogError("Unhandled Exception in Queue: {Queue}, CorrelationId: {CorrelationId}, MessageId: {MessageId}, Exception: {Exception}",
            QueueUrl, CorrelationIdContext.Value, message.MessageId, ex);

        await MoveToDlqAndNotifyObserver(message, queueUrl, ex, cancellationToken);
    }

    private async Task MoveToDlqAndNotifyObserver(Message message, string queueUrl, Exception ex, CancellationToken cancellationToken)
    {
        await queueAdminService.MoveToDeadLetterQueueAsync(message, queueUrl, ex, cancellationToken);
        observer?.OnMessageFailed(message.MessageId, DateTime.UtcNow, ex, message);
    }

    private static int GetReceiveCount(Message message)
    {
        if (message.Attributes?.TryGetValue("ApproximateReceiveCount", out var countStr) == true
            && int.TryParse(countStr, out var count))
        {
            return count;
        }
        return 0;
    }
}
