using Amazon.SQS;
using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Consumers;
using CadsBridge.Application.Messaging.Messages;
using CadsBridge.Application.Messaging.Models;
using CadsBridge.Application.Messaging.Observers;
using CadsBridge.Application.Messaging.Services;
using CadsBridge.Core.Correlation;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Messaging.Configuration;
using CadsBridge.Infrastructure.Messaging.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CadsBridge.Infrastructure.Messaging.Consumers;

public abstract class BaseSqsQueuePoller<TClient>(
    IServiceScopeFactory scopeFactory,
    IAmazonSQS sqs,
    IOptionsMonitor<QueueConsumerOptions> options,
    TClient client,
    IQueueAdminService<TClient> queueAdminService,
    IQueuePollerObserver<MessageType> observer,
    ILogger logger) : IQueuePoller<TClient>, IAsyncDisposable
    where TClient : IQueueClient
{
    protected readonly IServiceScopeFactory ScopeFactory = scopeFactory;
    protected readonly IAmazonSQS Sqs = sqs;
    protected readonly IQueueAdminService<TClient> QueueAdminService = queueAdminService;
    protected readonly IQueuePollerObserver<MessageType> Observer = observer;
    protected readonly ILogger Logger = logger;
    protected readonly QueueConsumerOptions Options = options.Get(client.ClientName);

    private Task? _pollingTask;
    private CancellationTokenSource _cts = new();

    public string QueueUrl => Options.QueueUrl;
    public string? DlqQueueUrl => Options.DlqQueueUrl;

    public Task StartAsync(CancellationToken token)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(token);

        _pollingTask = Task.Factory.StartNew(
            () => PollMessagesAsync(_cts.Token),
            _cts.Token,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken token)
    {
        await _cts.CancelAsync();

        if (_pollingTask is { IsCompletedSuccessfully: false })
        {
            try { await _pollingTask; }
            catch (TaskCanceledException) { }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None);
        _cts.Dispose();
        GC.SuppressFinalize(this);
    }

    private async Task PollMessagesAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var response = await Sqs.ReceiveMessageAsync(BuildReceiveRequest(), cancellationToken)
                    .ConfigureAwait(false);

                var messages = response?.Messages;

                if (messages == null || messages.Count == 0) continue;

                foreach (var message in messages)
                {
                    await HandleMessageAsync(message, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Polling error for queue {QueueUrl}", QueueUrl);
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }

            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }
    }

    private ReceiveMessageRequest BuildReceiveRequest() => new()
    {
        QueueUrl = QueueUrl,
        MaxNumberOfMessages = Options.MaxNumberOfMessages,
        WaitTimeSeconds = Options.WaitTimeSeconds,
        MessageAttributeNames = ["All"],
        MessageSystemAttributeNames = ["All"]
    };

    private async Task HandleMessageAsync(Message message, CancellationToken cancellationToken)
    {
        var unwrapped = message.Unwrap();

        CorrelationIdContext.Value = string.IsNullOrWhiteSpace(unwrapped.CorrelationId)
            ? Guid.NewGuid().ToString()
            : unwrapped.CorrelationId;

        try
        {
            var result = await ProcessMessageAsync(unwrapped, cancellationToken).ConfigureAwait(false);

            await Sqs.DeleteMessageAsync(QueueUrl, message.ReceiptHandle, cancellationToken)
                .ConfigureAwait(false);

            if (Logger.IsEnabled(LogLevel.Information))
            {
                Logger.LogInformation(
                    "Handled message with CorrelationId: {CorrelationId}, GroupId={GroupId}, DedupId={DedupId}",
                    CorrelationIdContext.Value, unwrapped.MessageGroupId, unwrapped.MessageDeduplicationId);
            }

            Observer?.OnMessageHandled(message.MessageId, DateTime.UtcNow, result, message);
        }
        catch (RetryableException ex)
        {
            HandleRetryableException(message, unwrapped, ex);
        }
        catch (NonRetryableException ex)
        {
            await HandleNonRetryableException(message, unwrapped, ex, cancellationToken);
        }
        catch (Exception ex)
        {
            await HandleUnexpectedException(message, unwrapped, ex, cancellationToken);
        }
    }

    protected abstract Task<MessageType?> ProcessMessageAsync(UnwrappedMessage message, CancellationToken cancellationToken);

    protected void HandleRetryableException(
        Message rawMessage,
        UnwrappedMessage unwrapped,
        RetryableException ex)
    {
        var receiveCount = GetReceiveCount(rawMessage);

        Logger.LogWarning(
            ex,
            "RetryableException in Queue={Queue}, CorrelationId={CorrelationId}, GroupId={GroupId}, DedupId={DedupId}, MessageId={MessageId}, ReceiveCount={ReceiveCount}",
            QueueUrl,
            CorrelationIdContext.Value,
            unwrapped.MessageGroupId,
            unwrapped.MessageDeduplicationId,
            rawMessage.MessageId,
            receiveCount);

        Observer?.OnMessageFailed(rawMessage.MessageId, DateTime.UtcNow, ex, rawMessage);
    }

    protected async Task HandleNonRetryableException(
        Message rawMessage,
        UnwrappedMessage unwrapped,
        NonRetryableException ex,
        CancellationToken cancellationToken)
    {
        Logger.LogError(
            ex,
            "NonRetryableException in Queue={Queue}, CorrelationId={CorrelationId}, GroupId={GroupId}, DedupId={DedupId}, MessageId={MessageId}",
            QueueUrl,
            CorrelationIdContext.Value,
            unwrapped.MessageGroupId,
            unwrapped.MessageDeduplicationId,
            rawMessage.MessageId);

        await MoveToDlqAndNotifyObserver(rawMessage, ex, cancellationToken);
    }

    protected async Task HandleUnexpectedException(
        Message rawMessage,
        UnwrappedMessage unwrapped,
        Exception ex,
        CancellationToken cancellationToken)
    {
        Logger.LogError(
            ex,
            "UnhandledException in Queue={Queue}, CorrelationId={CorrelationId}, GroupId={GroupId}, DedupId={DedupId}, MessageId={MessageId}",
            QueueUrl,
            CorrelationIdContext.Value,
            unwrapped.MessageGroupId,
            unwrapped.MessageDeduplicationId,
            rawMessage.MessageId);

        await MoveToDlqAndNotifyObserver(rawMessage, ex, cancellationToken);
    }

    protected async Task MoveToDlqAndNotifyObserver(
        Message rawMessage,
        Exception ex,
        CancellationToken cancellationToken)
    {
        await QueueAdminService.MoveToDeadLetterQueueAsync(
            rawMessage,
            QueueUrl,
            DlqQueueUrl,
            ex,
            cancellationToken);

        Observer?.OnMessageFailed(rawMessage.MessageId, DateTime.UtcNow, ex, rawMessage);
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