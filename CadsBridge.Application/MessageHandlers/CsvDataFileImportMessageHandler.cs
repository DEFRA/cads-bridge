using CadsBridge.Application.Commands;
using CadsBridge.Application.Commands.MessageProcessing;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.Messaging.Messages;
using CadsBridge.Application.Messaging.Serializers;
using CadsBridge.Core.Exceptions;
using System.Threading.Channels;

namespace CadsBridge.Application.MessageHandlers;

public class CsvDataFileImportMessageHandler(
    Channel<CsvDataFileImportJob> channel,
    IUnwrappedMessageSerializer<CsvDataFileImportMessage> serializer)
    : ICommandHandler<ProcessCsvDataFileImportMessageCommand, MessageType>
{
    private readonly Channel<CsvDataFileImportJob> _channel = channel;
    private readonly IUnwrappedMessageSerializer<CsvDataFileImportMessage> _serializer = serializer;

    public async Task<MessageType> Handle(ProcessCsvDataFileImportMessageCommand request, CancellationToken cancellationToken)
    {
        var message = request.Message;

        ArgumentNullException.ThrowIfNull(message);

        var messagePayload = _serializer.Deserialize(message)
            ?? throw new NonRetryableException($"Deserialisation failed or the message payload was null for " +
            $"messageType: {typeof(CsvDataFileImportMessage).Name}," +
            $"messageId: {message.MessageId}," +
            $"correlationId: {message.CorrelationId}");

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

        return await Task.FromResult(messagePayload);
    }
}