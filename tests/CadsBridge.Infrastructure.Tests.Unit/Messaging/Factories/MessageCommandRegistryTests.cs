using CadsBridge.Application.Commands.MessageProcessing;
using CadsBridge.Application.Messaging.Commands;
using CadsBridge.Application.Messaging.Models;
using CadsBridge.Infrastructure.Messaging.Factories;
using FluentAssertions;

namespace CadsBridge.Infrastructure.Tests.Unit.Messaging.Factories;

public class MessageCommandRegistryTests
{
    private readonly MessageCommandRegistry _registry = new();

    private const string CsvSubject = "CsvDataFileImport";
    private const string OtherSubject = "OtherMessage";

    [Fact]
    public void Register_ShouldStoreFactoryAgainstSubject()
    {
        _registry.Register<CsvDataFileImportMessageCommandFactory>(CsvSubject);

        var message = new UnwrappedMessage
        {
            Subject = CsvSubject,
            MessageId = "123",
            Payload = "{}"
        };

        var command = _registry.CreateCommand(message);

        command.Should().BeOfType<ProcessCsvDataFileImportMessageCommand>();
    }

    [Fact]
    public void CreateCommand_ShouldThrow_WhenSubjectNotRegistered()
    {
        var message = new UnwrappedMessage
        {
            Subject = "UnknownSubject",
            MessageId = "123",
            Payload = "{}"
        };

        var act = () => _registry.CreateCommand(message);

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("No command registered for subject UnknownSubject");
    }

    [Fact]
    public void CreateCommand_ShouldPassUnwrappedMessageToFactory()
    {
        _registry.Register<CsvDataFileImportMessageCommandFactory>(CsvSubject);

        var message = new UnwrappedMessage
        {
            Subject = CsvSubject,
            MessageId = "ABC",
            Payload = "{\"test\":1}",
            CorrelationId = "Corr-123"
        };

        var command = _registry.CreateCommand(message);

        command.Should().BeOfType<ProcessCsvDataFileImportMessageCommand>();

        var typed = (ProcessCsvDataFileImportMessageCommand)command;
        typed.Message.MessageId.Should().Be("ABC");
        typed.Message.Payload.Should().Be("{\"test\":1}");
        typed.Message.CorrelationId.Should().Be("Corr-123");
    }

    [Fact]
    public void Register_ShouldOverrideExistingFactory_WhenSameSubjectIsUsed()
    {
        _registry.Register<CsvDataFileImportMessageCommandFactory>(CsvSubject);
        _registry.Register<TestMessageCommandFactory>(CsvSubject); // override

        var message = new UnwrappedMessage
        {
            Subject = CsvSubject,
            MessageId = "XYZ",
            Payload = "{}"
        };

        var command = _registry.CreateCommand(message);

        command.Should().BeOfType<TestMessageProcessingCommand>();
    }

    [Fact]
    public void Registry_ShouldSupportMultipleSubjects()
    {
        _registry.Register<CsvDataFileImportMessageCommandFactory>(CsvSubject);
        _registry.Register<TestMessageCommandFactory>(OtherSubject);

        var csvMessage = new UnwrappedMessage { Subject = CsvSubject, MessageId = "1", Payload = "{}" };
        var otherMessage = new UnwrappedMessage { Subject = OtherSubject, MessageId = "2", Payload = "{}" };

        _registry.CreateCommand(csvMessage).Should().BeOfType<ProcessCsvDataFileImportMessageCommand>();
        _registry.CreateCommand(otherMessage).Should().BeOfType<TestMessageProcessingCommand>();
    }

    private sealed class TestMessageCommandFactory : IMessageCommandFactory
    {
        public IMessageProcessingCommand Create(UnwrappedMessage message)
            => new TestMessageProcessingCommand(message);
    }

    private sealed class TestMessageProcessingCommand(UnwrappedMessage message) : IMessageProcessingCommand
    {
        public UnwrappedMessage Message { get; } = message;

        public static Task ExecuteAsync()
            => Task.CompletedTask;
    }
}