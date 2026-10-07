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
        var queueOptions = ResolveQueueOptions(queue);

        if (string.IsNullOrWhiteSpace(queueOptions.DlqQueueUrl))
        {
            throw new UnprocessableException($"Queue '{queue}' does not have a configured DLQ.");
        }

        return await GetMetricsForUrlAsync(queueOptions.DlqQueueUrl, $"{queue} DLQ", cancellationToken);
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
        var queueOptions = ResolveQueueOptions(request.Queue);

        if (string.IsNullOrWhiteSpace(queueOptions.DlqQueueUrl))
        {
            throw new UnprocessableException($"Queue '{request.Queue}' does not have a configured DLQ.");
        }

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug("[SqsAdminService] Peeking DLQ messages for queue {Queue}", request.Queue);
        }

        return await PeekFromUrlAsync(queueOptions.DlqQueueUrl, request.MaxMessages, cancellationToken);
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
        var queueOptions = ResolveQueueOptions(request.Queue);

        if (string.IsNullOrWhiteSpace(queueOptions.DlqQueueUrl))
        {
            throw new UnprocessableException($"Queue '{request.Queue}' does not have a configured DLQ.");
        }

        var dlqUrl = queueOptions.DlqQueueUrl;
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
                try
                {
                    await sqs.SendMessageAsync(
                        new SendMessageRequest
                        {
                            QueueUrl = mainQueueUrl,
                            MessageGroupId = message.Attributes?.GetValueOrDefault("MessageGroupId"),
                            MessageDeduplicationId = message.Attributes?.GetValueOrDefault("MessageDeduplicationId"),
                            MessageBody = message.Body,
                            MessageAttributes = message.MessageAttributes
                        },
                        cancellationToken);

                    await sqs.DeleteMessageAsync(dlqUrl, message.ReceiptHandle, cancellationToken);

                    moved++;

                    if (logger.IsEnabled(LogLevel.Information))
                    {
                        logger.LogInformation(
                            "DLQ replay: moved MessageId={MessageId} from {DlqUrl} to {MainQueueUrl}",
                            message.MessageId, dlqUrl, mainQueueUrl);
                    }
                }
                catch (Exception ex)
                {
                    // Graceful partial replay: leave the message on the DLQ (its visibility
                    // timeout will expire, allowing a later retry) and keep processing the batch.
                    errors.Add($"MessageId '{message.MessageId}': {ex.Message}");

                    logger.LogError(
                        ex,
                        "DLQ replay failed for MessageId={MessageId} on {DlqUrl}",
                        message.MessageId, dlqUrl);
                }
            }
        }

        return new ReplayResultDto(moved, errors.Count, errors);
    }

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

    private Dictionary<string, SqsAdminQueueOptions> GetConfiguredQueues() =>
        options.CurrentValue;
}