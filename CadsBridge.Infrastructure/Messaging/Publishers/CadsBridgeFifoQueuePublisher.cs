using Amazon.SQS;
using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Publishers;
using CadsBridge.Core.Correlation;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Messaging.Configuration;
using CadsBridge.Infrastructure.Messaging.Factories;
using Microsoft.Extensions.Options;

namespace CadsBridge.Infrastructure.Messaging.Publishers;

public class CadsBridgeFifoQueuePublisher(
    IAmazonSQS sqs,
    IMessageFactory messageFactory,
    IOptionsMonitor<QueuePublisherOptions> options,
    CadsBridgeFifoQueueClient client)
    : IMessagePublisher<CadsBridgeFifoQueueClient>
{
    private readonly IAmazonSQS _sqs = sqs;
    private readonly IMessageFactory _messageFactory = messageFactory;
    private readonly IOptionsMonitor<QueuePublisherOptions> _options = options;
    private readonly CadsBridgeFifoQueueClient _client = client;

    public string QueueUrl => _options.Get(_client.ClientName).QueueUrl;

    public async Task PublishAsync<TMessage>(TMessage? message, CancellationToken cancellationToken = default)
        where TMessage : class
    {
        if (message == null) throw new ArgumentException("Message payload was null", nameof(message));

        if (string.IsNullOrWhiteSpace(QueueUrl)) throw new PublishFailedException("QueueUrl is missing", false);

        // TODO: Replace values
        var dedupId = FifoKeyGenerator.GenerateDeduplicationId(
            "payload.Bucket",
            "payload.ObjectKey",
            "payload.Etag",
            "payload.ImportType",
            "payload.OracleEnvironment");

        // TODO: Replace values
        var groupId = FifoKeyGenerator.GenerateMessageGroupId(
            "payload.ImportType",
            "payload.OracleEnvironment");

        var correlationId = CorrelationIdContext.Value ?? Guid.NewGuid().ToString();

        var metadata = new FifoMessageMetadata(
            messageGroupId: groupId,
            messageDeduplicationId: dedupId,
            correlationId: correlationId);

        try
        {
            var sendRequest = _messageFactory.CreateFifoSqsMessage(QueueUrl, message, metadata);

            await _sqs.SendMessageAsync(sendRequest, cancellationToken);
        }
        catch (Exception ex)
        {
            throw new PublishFailedException($"Failed to publish message on {QueueUrl}.", false, ex);
        }
    }
}