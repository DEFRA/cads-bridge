using CadsBridge.Application.Commands.MessageProcessing;
using CadsBridge.Application.Messaging.Commands;
using CadsBridge.Application.Messaging.Models;

namespace CadsBridge.Infrastructure.Messaging.Factories;

public sealed class MessageCommandRegistry
{
    private readonly Dictionary<string, IMessageCommandFactory> _map = [];

    public void Register<TFactory>(string subject)
        where TFactory : IMessageCommandFactory, new()
    {
        _map[subject] = new TFactory();
    }

    public IMessageProcessingCommand CreateCommand(UnwrappedMessage message)
    {
        if (!_map.TryGetValue(message.Subject, out var factory))
            throw new InvalidOperationException($"No command registered for subject {message.Subject}");

        return factory.Create(message);
    }
}

public sealed class CsvDataFileImportMessageCommandFactory : IMessageCommandFactory
{
    public IMessageProcessingCommand Create(UnwrappedMessage message)
        => new ProcessCsvDataFileImportMessageCommand(message);
}