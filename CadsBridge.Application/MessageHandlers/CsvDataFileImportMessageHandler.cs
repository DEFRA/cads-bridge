using CadsBridge.Application.Commands;
using CadsBridge.Application.Commands.MessageProcessing;
using CadsBridge.Application.Messaging.Messages;
using CadsBridge.Application.Messaging.Serializers;
using CadsBridge.Core.Exceptions;

namespace CadsBridge.Application.MessageHandlers;

public class CsvDataFileImportMessageHandler(
    IUnwrappedMessageSerializer<CsvDataFileImportMessage> serializer)
    : ICommandHandler<ProcessCsvDataFileImportMessageCommand, MessageType>
{
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

        // DO WORK

        return await Task.FromResult(messagePayload);
    }
}