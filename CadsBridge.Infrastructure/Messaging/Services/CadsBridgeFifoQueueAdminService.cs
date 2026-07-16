using Amazon.SQS;
using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Constants;
using CadsBridge.Application.Messaging.Services;
using CadsBridge.Core.Correlation;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.Messaging.Services;

public class CadsBridgeFifoQueueAdminService(
    IAmazonSQS sqs,
    ILogger<CadsBridgeFifoQueueAdminService> logger)
    : IQueueAdminService<CadsBridgeFifoQueueClient>
{
    public async Task<bool> MoveToDeadLetterQueueAsync(
        Message message,
        string queueUrl,
        string? dlqQueueUrl,
        Exception ex,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(dlqQueueUrl))
        {
            logger.LogWarning(DeadLetterQueueServiceConstants.LogMessages.NoDlqConfigured, message.MessageId);
            return false;
        }

        var sendSucceeded = false;
        var deleteSucceeded = false;

        try
        {
            var attributes = new Dictionary<string, MessageAttributeValue>(message.MessageAttributes ?? [])
            {
                [DeadLetterQueueServiceConstants.MessageAttributes.DlqFailureReason] = new()
                {
                    StringValue = ex.GetType().Name,
                    DataType = DeadLetterQueueServiceConstants.StringDataType
                },
                [DeadLetterQueueServiceConstants.MessageAttributes.DlqFailureMessage] = new()
                {
                    StringValue = ex.Message[..Math.Min(DeadLetterQueueServiceConstants.Limits.MaxSqsMessageAttributeLength, ex.Message.Length)],
                    DataType = DeadLetterQueueServiceConstants.StringDataType
                },
                [DeadLetterQueueServiceConstants.MessageAttributes.DlqFailureTimestamp] = new()
                {
                    StringValue = DateTime.UtcNow.ToString("O"),
                    DataType = DeadLetterQueueServiceConstants.StringDataType
                },
                [DeadLetterQueueServiceConstants.MessageAttributes.DlqOriginalMessageId] = new()
                {
                    StringValue = message.MessageId,
                    DataType = DeadLetterQueueServiceConstants.StringDataType
                },
                [DeadLetterQueueServiceConstants.MessageAttributes.DlqReceiveCount] = new()
                {
                    StringValue = (message.Attributes ?? []).GetValueOrDefault(DeadLetterQueueServiceConstants.SqsAttributes.ApproximateReceiveCount, "0"),
                    DataType = DeadLetterQueueServiceConstants.NumberDataType
                }
            };

            if (!attributes.ContainsKey(DeadLetterQueueServiceConstants.MessageAttributes.CorrelationId))
            {
                attributes[DeadLetterQueueServiceConstants.MessageAttributes.CorrelationId] = new MessageAttributeValue
                {
                    DataType = DeadLetterQueueServiceConstants.StringDataType,
                    StringValue = CorrelationIdContext.Value
                };
            }

            var sendRequest = new SendMessageRequest
            {
                QueueUrl = dlqQueueUrl,
                MessageBody = message.Body,
                MessageAttributes = attributes
            };

            var sendResponse = await sqs.SendMessageAsync(sendRequest, cancellationToken);
            sendSucceeded = true;

            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation(DeadLetterQueueServiceConstants.LogMessages.SentToDlq, message.MessageId, sendResponse.MessageId);
            }

            await sqs.DeleteMessageAsync(queueUrl, message.ReceiptHandle, cancellationToken);
            deleteSucceeded = true;

            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation(DeadLetterQueueServiceConstants.LogMessages.MovedToDlq, message.MessageId);
            }

            return true;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception dlqEx)
        {
            var status = sendSucceeded
                ? DeadLetterQueueServiceConstants.LogMessages.SendSucceededDeleteFailed
                : DeadLetterQueueServiceConstants.LogMessages.FailedToSend;

            logger.LogError(dlqEx, "{Status}. MessageId: {MessageId}, SendSucceeded: {SendSucceeded}, DeleteSucceeded: {DeleteSucceeded}",
                status, message.MessageId, sendSucceeded, deleteSucceeded);

            if (sendSucceeded && !deleteSucceeded)
            {
                logger.LogError(DeadLetterQueueServiceConstants.LogMessages.DeleteFailed, message.MessageId);
            }

            return false;
        }
    }
}