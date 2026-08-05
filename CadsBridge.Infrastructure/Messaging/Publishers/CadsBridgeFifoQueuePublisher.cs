using Amazon.SQS;
using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Models;
using CadsBridge.Application.Messaging.Publishers;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Messaging.Configuration;
using CadsBridge.Infrastructure.Messaging.Factories;
using Microsoft.Extensions.Options;
using System.Net;

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

    public async Task PublishAsync<TMessage>(TMessage? message, FifoMessageMetadata metadata, CancellationToken cancellationToken = default)
        where TMessage : class
    {
        if (message == null) throw new ArgumentException("Message payload was null", nameof(message));

        if (string.IsNullOrWhiteSpace(QueueUrl)) throw new PublishFailedException("QueueUrl is missing", false);

        try
        {
            var sendRequest = _messageFactory.CreateFifoSqsMessage(QueueUrl, message, metadata);

            await _sqs.SendMessageAsync(sendRequest, cancellationToken);
        }
        catch (Exception ex)
        {
            var isTransient = ex is AmazonSQSException sqsEx &&
                              sqsEx.StatusCode is >= HttpStatusCode.InternalServerError
                                  or HttpStatusCode.TooManyRequests;
            throw new PublishFailedException($"Failed to publish message on {QueueUrl}.", isTransient, ex);
        }
    }
}