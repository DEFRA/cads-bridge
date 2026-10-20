using Amazon.SQS;
using Amazon.SQS.Model;
using CadsBridge.Application.SqsAdmin.Services;
using CadsBridge.Core.DTOs.SqsAdmin;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.SqsAdmin.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CadsBridge.Infrastructure.SqsAdmin.Services;

public class SqsAdminService(
    IAmazonSQS sqs,
    IOptionsMonitor<Dictionary<string, SqsAdminQueueOptions>> options,
    ILogger<SqsAdminService> logger) : ISqsAdminService
{
    private const int MaxMessagesPerSqsRequest = 10;

    // Upper bound on how many batches we'll scan through when searching the DLQ for a
    // specific MessageId, so a missing message can't cause an unbounded loop.
    private const int MaxSingleMessageSearchAttempts = 20;

    private static readonly string[] MetricAttributeNames =
    [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed"
    ];

    public Task<IReadOnlyList<QueueInfoDto>> GetQueuesAsync(CancellationToken cancellationToken = default)
    {
        IReadOnlyList<QueueInfoDto> queues = GetConfiguredQueues()
            .Select(q => new QueueInfoDto(q.Key, q.Value.QueueUrl, q.Value.DlqQueueUrl))
            .OrderBy(q => q.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return Task.FromResult(queues);
    }

    public Task<QueueMetricsDto> GetMetricsAsync(string queue, CancellationToken cancellationToken = default) =>
        GetMetricsForUrlAsync(ResolveQueueUrl(queue), queue, cancellationToken);

    public async Task<QueueMetricsDto> GetDlqMetricsAsync(string queue, CancellationToken cancellationToken = default)
    {
        var queueOptions = ResolveDlqQueueOptions(queue);

        return await GetMetricsForUrlAsync(queueOptions.DlqQueueUrl!, $"{queue} DLQ", cancellationToken);
    }

    private async Task<QueueMetricsDto> GetMetricsForUrlAsync(string queueUrl, string queue, CancellationToken cancellationToken)
    {
        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug("[SqsAdminService] Getting metrics for queue {Queue} from attributes", queue);
        }

        var attributesResponse = await sqs.GetQueueAttributesAsync(
            new GetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                AttributeNames = [.. MetricAttributeNames]
            },
            cancellationToken);

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug("[SqsAdminService] Getting oldest message for queue {Queue} from first out message", queue);
        }

        var oldestMessageAgeSeconds = await GetOldestMessageAgeSecondsAsync(queueUrl, cancellationToken);

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug("[SqsAdminService] Retrieved metrics for queue {Queue}", queue);
        }

        return new QueueMetricsDto(
            GetLongAttribute(attributesResponse.Attributes, "ApproximateNumberOfMessages"),
            GetLongAttribute(attributesResponse.Attributes, "ApproximateNumberOfMessagesNotVisible"),
            GetLongAttribute(attributesResponse.Attributes, "ApproximateNumberOfMessagesDelayed"),
            oldestMessageAgeSeconds);
    }

    public Task<IReadOnlyList<QueueMessageDto>> PeekMessagesAsync(
        PeekMessagesRequestDto request,
        CancellationToken cancellationToken = default) =>
        PeekFromUrlAsync(ResolveQueueUrl(request.Queue), request.MaxMessages, cancellationToken);

    public async Task<IReadOnlyList<QueueMessageDto>> PeekDlqMessagesAsync(
        PeekMessagesRequestDto request,
        CancellationToken cancellationToken = default)
    {
        var queueOptions = ResolveDlqQueueOptions(request.Queue);

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug("[SqsAdminService] Peeking DLQ messages for queue {Queue}", request.Queue);
        }

        return await PeekFromUrlAsync(queueOptions.DlqQueueUrl!, request.MaxMessages, cancellationToken);
    }

    private async Task<IReadOnlyList<QueueMessageDto>> PeekFromUrlAsync(string queueUrl, int maxMessages, CancellationToken cancellationToken)
    {
        var response = await GetMessagesFromQueue(queueUrl, maxMessages, cancellationToken: cancellationToken);

        return response.Messages
            .Select(MapToQueueMessageDto)
            .ToList();
    }

    private async Task<ReceiveMessageResponse> GetMessagesFromQueue(string queueUrl, int maxMessages, int visibilityTimeout = 0, List<string>? systemAttributeNames = null, CancellationToken cancellationToken = default)
    {
        // Having a default visibility timeout of 0 allows us to peek at messages without affecting their visibility in the queue.
        // Setting WaitTimeSeconds to 0 ensures that the request returns immediately, even if there are no messages available, which is important for peeking.
        var response = await sqs.ReceiveMessageAsync(
            new ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                MaxNumberOfMessages = Math.Clamp(maxMessages, 1, MaxMessagesPerSqsRequest),
                VisibilityTimeout = visibilityTimeout,
                WaitTimeSeconds = 0,
                MessageAttributeNames = ["All"],
                MessageSystemAttributeNames = systemAttributeNames ?? ["All"]
            },
            cancellationToken);

        // Normalise here so callers can rely on it never being null.
        // Required due to localstack's SQS emulation not reliably supporting FIFO queues.
        response.Messages ??= [];

        return response;
    }

    public async Task<ReplayResultDto> ReplayDlqAsync(ReplayDlqRequestDto request, CancellationToken cancellationToken = default)
    {
        var queueOptions = ResolveDlqQueueOptions(request.Queue);

        var dlqUrl = queueOptions.DlqQueueUrl!;
        var mainQueueUrl = queueOptions.QueueUrl;
        var batchSize = request.BatchSize > 0 ? request.BatchSize : queueOptions.DefaultReplayBatchSize;

        var moved = 0;
        var errors = new List<string>();

        while (moved + errors.Count < batchSize)
        {
            var remaining = batchSize - (moved + errors.Count);

            var receiveResponse = await GetMessagesFromQueue(dlqUrl, remaining, visibilityTimeout: 30, cancellationToken: cancellationToken);

            if (receiveResponse.Messages.Count == 0)
            {
                break;
            }

            foreach (var message in receiveResponse.Messages)
            {
                var (success, error) = await TryReplayMessageAsync(message, dlqUrl, mainQueueUrl, cancellationToken);

                if (success)
                {
                    moved++;
                }
                else
                {
                    errors.Add(error!);
                }
            }
        }

        return new ReplayResultDto(moved, errors.Count, errors);
    }

    public async Task<ReplayResultDto> ReplayDlqMessageAsync(ReplayDlqMessageRequestDto request, CancellationToken cancellationToken = default)
    {
        var queueOptions = ResolveDlqQueueOptions(request.Queue);

        var dlqUrl = queueOptions.DlqQueueUrl!;
        var mainQueueUrl = queueOptions.QueueUrl;

        // Messages we received while searching for the target but didn't end up replaying.
        // Receiving them hides them (via the visibility timeout below) even though they're
        // not being actioned, so we must make them visible again as soon as we're done
        // searching rather than leaving them hidden from other consumers/metrics for the
        // full 30s visibility timeout.
        var skippedReceiptHandles = new List<string>();

        try
        {
            for (var attempt = 0; attempt < MaxSingleMessageSearchAttempts; attempt++)
            {
                var receiveResponse = await GetMessagesFromQueue(dlqUrl, MaxMessagesPerSqsRequest, visibilityTimeout: 30, cancellationToken: cancellationToken);

                if (receiveResponse.Messages.Count == 0)
                {
                    break;
                }

                var message = receiveResponse.Messages.FirstOrDefault(m => m.MessageId == request.MessageId);

                skippedReceiptHandles.AddRange(receiveResponse.Messages
                    .Where(m => m != message)
                    .Select(m => m.ReceiptHandle));

                if (message != null)
                {
                    var (success, error) = await TryReplayMessageAsync(message, dlqUrl, mainQueueUrl, cancellationToken);

                    return success
                        ? new ReplayResultDto(1, 0, [])
                        : new ReplayResultDto(0, 1, [error!]);
                }
            }

            throw new NotFoundException("DLQ message", request.MessageId);
        }
        finally
        {
            await ReleaseSkippedMessagesAsync(dlqUrl, skippedReceiptHandles, cancellationToken);
        }
    }

    // Immediately resets the visibility timeout of messages that were incidentally hidden
    // while searching for a single target message, so they re-appear on the queue (and in
    // metrics) straight away instead of staying hidden for the full visibility timeout.
    // Best-effort: if this fails, the messages simply remain hidden until the timeout
    // naturally expires, which is not fatal.
    private async Task ReleaseSkippedMessagesAsync(string queueUrl, IReadOnlyCollection<string> receiptHandles, CancellationToken cancellationToken)
    {
        if (receiptHandles.Count == 0)
        {
            return;
        }

        foreach (var batch in receiptHandles.Chunk(MaxMessagesPerSqsRequest))
        {
            try
            {
                var entries = batch
                    .Select((receiptHandle, index) => new ChangeMessageVisibilityBatchRequestEntry
                    {
                        Id = index.ToString(),
                        ReceiptHandle = receiptHandle,
                        VisibilityTimeout = 0
                    })
                    .ToList();

                await sqs.ChangeMessageVisibilityBatchAsync(
                    new ChangeMessageVisibilityBatchRequest
                    {
                        QueueUrl = queueUrl,
                        Entries = entries
                    },
                    cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "Failed to release {Count} skipped DLQ messages early on {QueueUrl}",
                    batch.Length, queueUrl);
            }
        }
    }

    private async Task<(bool Success, string? Error)> TryReplayMessageAsync(
        Message message,
        string dlqUrl,
        string mainQueueUrl,
        CancellationToken cancellationToken)
    {
        try
        {
            await sqs.SendMessageAsync(
                new SendMessageRequest
                {
                    QueueUrl = mainQueueUrl,
                    MessageGroupId = ResolveMessageAttribute(message, "MessageGroupId"),
                    MessageDeduplicationId = ResolveMessageAttribute(message, "MessageDeduplicationId"),
                    MessageBody = message.Body,
                    MessageAttributes = message.MessageAttributes
                },
                cancellationToken);

            await sqs.DeleteMessageAsync(dlqUrl, message.ReceiptHandle, cancellationToken);

            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation(
                    "DLQ replay: moved MessageId={MessageId} from {DlqUrl} to {MainQueueUrl}",
                    message.MessageId, dlqUrl, mainQueueUrl);
            }

            return (true, null);
        }
        catch (Exception ex)
        {
            // Graceful partial replay: leave the message on the DLQ (its visibility
            // timeout will expire, allowing a later retry) and keep processing the batch.
            logger.LogError(
                ex,
                "DLQ replay failed for MessageId={MessageId} on {DlqUrl}",
                message.MessageId, dlqUrl);

            return (false, $"MessageId '{message.MessageId}': {ex.Message}");
        }
    }

    // System attributes (requested via MessageSystemAttributeNames on ReceiveMessage) are
    // preferred, but LocalStack's FIFO queue emulation doesn't reliably return them, so we
    // fall back to the custom message attribute of the same name if the message was
    // published with one (as our own producers/tests do).
    private static string? ResolveMessageAttribute(Message message, string attributeName) =>
        message.Attributes?.GetValueOrDefault(attributeName)
        ?? message.MessageAttributes?.GetValueOrDefault(attributeName)?.StringValue;


    private async Task<long> GetOldestMessageAgeSecondsAsync(string queueUrl, CancellationToken cancellationToken)
    {
        var response = await GetMessagesFromQueue(queueUrl, 1, systemAttributeNames: ["SentTimestamp"], cancellationToken: cancellationToken);

        var message = response.Messages.FirstOrDefault();
        if (message == null) return 0;

        var messageAttributes = message.Attributes ?? [];

        if (!messageAttributes.TryGetValue("SentTimestamp", out var sentTimestampRaw) ||
            !long.TryParse(sentTimestampRaw, out var sentTimestampMs))
        {
            return 0;
        }

        var sentAt = DateTimeOffset.FromUnixTimeMilliseconds(sentTimestampMs);
        var age = DateTimeOffset.UtcNow - sentAt;

        return age.Ticks > 0 ? (long)age.TotalSeconds : 0;
    }

    private static QueueMessageDto MapToQueueMessageDto(Message message)
    {
        // Normalise here so callers can rely on it never being null.
        // Required due to localstack's SQS emulation not reliably supporting FIFO queues.
        var systemAttributes = message.Attributes ?? [];

        DateTimeOffset? sentTimestamp = null;
        if (systemAttributes.TryGetValue("SentTimestamp", out var sentTimestampRaw) &&
            long.TryParse(sentTimestampRaw, out var sentTimestampMs))
        {
            sentTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(sentTimestampMs);
        }

        var attributes = systemAttributes
            .ToDictionary(a => a.Key, a => a.Value);

        return new QueueMessageDto(message.MessageId, message.Body, attributes, sentTimestamp);
    }


    private static long GetLongAttribute(Dictionary<string, string> attributes, string key) =>
        attributes.TryGetValue(key, out var value) && long.TryParse(value, out var parsed) ? parsed : 0;

    private string ResolveQueueUrl(string queue) => ResolveQueueOptions(queue).QueueUrl;

    private SqsAdminQueueOptions ResolveQueueOptions(string queue)
    {
        var queues = GetConfiguredQueues();

        if (!queues.TryGetValue(queue, out var queueOptions))
        {
            throw new NotFoundException("Queue", queue);
        }

        return queueOptions;
    }

    private SqsAdminQueueOptions ResolveDlqQueueOptions(string queue)
    {
        var queueOptions = ResolveQueueOptions(queue);

        if (string.IsNullOrWhiteSpace(queueOptions.DlqQueueUrl))
        {
            throw new UnprocessableException($"Queue '{queue}' does not have a configured DLQ.");
        }

        return queueOptions;
    }

    private Dictionary<string, SqsAdminQueueOptions> GetConfiguredQueues() =>
        options.CurrentValue;
}