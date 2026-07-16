using CadsBridge.Application.Messaging.Commands;
using CadsBridge.Application.Messaging.Models;

namespace CadsBridge.Infrastructure.Messaging.Factories;

public interface IMessageCommandFactory
{
    IMessageProcessingCommand Create(UnwrappedMessage message);
}