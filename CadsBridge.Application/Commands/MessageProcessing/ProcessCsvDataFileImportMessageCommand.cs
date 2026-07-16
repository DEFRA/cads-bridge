using CadsBridge.Application.Messaging.Commands;
using CadsBridge.Application.Messaging.Models;

namespace CadsBridge.Application.Commands.MessageProcessing;

public sealed record ProcessCsvDataFileImportMessageCommand(UnwrappedMessage Message)
    : IMessageProcessingCommand;