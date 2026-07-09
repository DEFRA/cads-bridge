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
    IAmazonSQS sqs,
    MessageCommandRegistry messageCommandRegistry,
    IOptionsMonitor<QueueConsumerOptions> options,
    CadsBridgeFifoQueueClient client,
    IQueueAdminService<CadsBridgeFifoQueueClient> queueAdminService,
    IQueuePollerObserver<MessageType> observer,
    ILogger<CadsBridgeFifoQueuePoller> logger)
        : IQueuePoller<CadsBridgeFifoQueueClient>, IAsyncDisposable
{
    private readonly QueueConsumerOptions _queueOptions = options.Get(client.ClientName);

    private Task? _pollingTask;
    private CancellationTokenSource _cts = new();

    public string QueueUrl => _queueOptions.QueueUrl;
    public string? DlqQueueUrl => _queueOptions.DlqQueueUrl;
    public int MaxNumberOfMessages => _queueOptions.MaxNumberOfMessages;
    public int WaitTimeSeconds => _queueOptions.WaitTimeSeconds;

    public Task StartAsync(CancellationToken token)
    {
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("QueuePoller start requested.");
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(token);

        _pollingTask = Task.Run(() => PollMessagesAsync(_cts.Token), token);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken token)
    {
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("QueuePoller stop requested.");
        }

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
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("Connecting to queue: {QueueUrl}", QueueUrl);
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var response = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = QueueUrl,
                    MaxNumberOfMessages = MaxNumberOfMessages,
                    WaitTimeSeconds = WaitTimeSeconds,
                    MessageAttributeNames = ["All"],
                    MessageSystemAttributeNames = ["All"]
                }, cancellationToken);

                var messages = response?.Messages;

                if (messages == null || messages.Count == 0) continue;

                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.LogInformation("Completed receive for queue: {QueueUrl}, Number of messages: {count}",
                        QueueUrl, messages.Count);
                }

                foreach (var message in messages)
                {
                    await HandleMessageAsync(message, cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.LogInformation("Poll operation cancelled for queue {QueueUrl}", QueueUrl);
                }
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

    private async Task HandleMessageAsync(Message message, CancellationToken cancellationToken)
    {
        try
        {
            var unwrappedMessage = message.Unwrap();

            CorrelationIdContext.Value = string.IsNullOrWhiteSpace(unwrappedMessage.CorrelationId)
                ? Guid.NewGuid().ToString()
                : unwrappedMessage.CorrelationId;

            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation(
                    "HandleMessageAsync using CorrelationId: {CorrelationId}, GroupId={GroupId}, DedupId={DedupId}",
                    CorrelationIdContext.Value, unwrappedMessage.MessageGroupId, unwrappedMessage.MessageDeduplicationId);
            }

            var command = messageCommandRegistry.CreateCommand(unwrappedMessage);

            using var scope = scopeFactory.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
            var result = await mediator.Send(command, cancellationToken);

            await sqs.DeleteMessageAsync(QueueUrl, message.ReceiptHandle, cancellationToken);

            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation(
                    "Handled message with CorrelationId: {CorrelationId}, GroupId={GroupId}, DedupId={DedupId}",
                    CorrelationIdContext.Value, unwrappedMessage.MessageGroupId, unwrappedMessage.MessageDeduplicationId);
            }

            observer?.OnMessageHandled(message.MessageId, DateTime.UtcNow, result, message);
        }
        catch (RetryableException ex)
        {
            HandleRetryableException(message, ex);
        }
        catch (NonRetryableException ex)
        {
            await HandleNonRetryableException(message, ex, cancellationToken);
        }
        catch (Exception ex)
        {
            await HandleUnexpectedException(message, ex, cancellationToken);
        }
    }

    private void HandleRetryableException(Message message, RetryableException ex)
    {
        var receiveCount = GetReceiveCount(message);

        var unwrappedMessage = message.Unwrap();

        logger.LogWarning(
            "RetryableException in Queue: {Queue}, CorrelationId: {CorrelationId}, GroupId={GroupId}, DedupId={DedupId}, MessageId: {MessageId}, ReceiveCount: {ReceiveCount}, Exception: {Exception}",
            QueueUrl, CorrelationIdContext.Value, unwrappedMessage.MessageGroupId, unwrappedMessage.MessageDeduplicationId, message.MessageId, receiveCount, ex);

        observer?.OnMessageFailed(message.MessageId, DateTime.UtcNow, ex, message);
    }

    private async Task HandleNonRetryableException(Message message, NonRetryableException ex, CancellationToken cancellationToken)
    {
        var unwrappedMessage = message.Unwrap();

        logger.LogError(
            "NonRetryableException in Queue: {Queue}, CorrelationId: {CorrelationId}, GroupId={GroupId}, DedupId={DedupId}, MessageId: {MessageId}, Exception: {Exception}",
            QueueUrl, CorrelationIdContext.Value, unwrappedMessage.MessageGroupId, unwrappedMessage.MessageDeduplicationId, message.MessageId, ex);

        await MoveToDlqAndNotifyObserver(message, ex, cancellationToken);
    }

    private async Task HandleUnexpectedException(Message message, Exception ex, CancellationToken cancellationToken)
    {
        var unwrappedMessage = message.Unwrap();

        logger.LogError(
            "Unhandled Exception in Queue: {Queue}, CorrelationId: {CorrelationId}, GroupId={GroupId}, DedupId={DedupId}, MessageId: {MessageId}, Exception: {Exception}",
            QueueUrl, CorrelationIdContext.Value, unwrappedMessage.MessageGroupId, unwrappedMessage.MessageDeduplicationId, message.MessageId, ex);

        await MoveToDlqAndNotifyObserver(message, ex, cancellationToken);
    }

    private async Task MoveToDlqAndNotifyObserver(Message message, Exception ex, CancellationToken cancellationToken)
    {
        await queueAdminService.MoveToDeadLetterQueueAsync(message, QueueUrl, DlqQueueUrl, ex, cancellationToken);
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