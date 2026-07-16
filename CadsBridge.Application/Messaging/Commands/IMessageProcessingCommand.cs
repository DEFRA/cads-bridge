using CadsBridge.Application.Commands;
using CadsBridge.Application.Messaging.Messages;

namespace CadsBridge.Application.Messaging.Commands;

public interface IMessageProcessingCommand : ICommand<MessageType>
{
}