using CadsBridge.Application.Commands;
using CadsBridge.Application.Commands.MessageProcessing;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.Messaging.Messages;
using CadsBridge.Application.Messaging.Serializers;
using CadsBridge.Core.Exceptions;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace CadsBridge.Application.MessageHandlers;

public class CsvDataFileImportMessageHandler(
    Channel<CsvDataFileImportJob> channel,
    IUnwrappedMessageSerializer<CsvDataFileImportMessage> serializer,
    ILogger<CsvDataFileImportMessageHandler> logger)
    : ICommandHandler<ProcessCsvDataFileImportMessageCommand, MessageType>
{
    private readonly Channel<CsvDataFileImportJob> _channel = channel;
    private readonly IUnwrappedMessageSerializer<CsvDataFileImportMessage> _serializer = serializer;

    public async Task<MessageType> Handle(ProcessCsvDataFileImportMessageCommand request, CancellationToken cancellationToken)
    {
        var message = request.Message;

        ArgumentNullException.ThrowIfNull(message);

        using (logger.BeginScope(new Dictionary<string, object?>
        {
            ["CorrelationId"] = message.CorrelationId
        }))
        {
            var messagePayload = _serializer.Deserialize(message)
                ?? throw new NonRetryableException($"Deserialisation failed or the message payload was null for " +
                $"messageType: {typeof(CsvDataFileImportMessage).Name}," +
                $"messageId: {message.MessageId}," +
                $"correlationId: {message.CorrelationId}");

            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("Enqueueing CsvDataFileImportJob with ObjectKey={ObjectKey}",
                    messagePayload.ObjectKey);
            }

            try
            {
                await _channel.Writer.WriteAsync(
                    new CsvDataFileImportJob(messagePayload.ObjectKey, messagePayload.CorrelationId), cancellationToken);
            }
            catch (ChannelClosedException ex)
            {
                throw new NonRetryableException(
                    $"Channel was closed while writing job for ObjectKey={messagePayload.ObjectKey}",
                    ex);
            }
            catch (OperationCanceledException)
            {
                // Let cancellation bubble up normally
                throw;
            }
            catch (Exception ex)
            {
                // Anything else is transient
                throw new RetryableException(
                    $"Failed to enqueue job for ObjectKey={messagePayload.ObjectKey}. Will retry.",
                    ex);
            }

            return messagePayload;
        }
    }
}