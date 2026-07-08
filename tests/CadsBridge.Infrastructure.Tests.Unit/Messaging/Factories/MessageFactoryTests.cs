using CadsBridge.Application.Messaging.Models;
using CadsBridge.Infrastructure.Messaging.Factories;
using CadsBridge.Testing.Support.Constants;
using FluentAssertions;

namespace CadsBridge.Infrastructure.Tests.Unit.Messaging.Factories;

public class MessageFactoryTests
{
    private readonly MessageFactory _factory = new();

    [Fact]
    public void GivenTestMessage_WhenCallingCreateFifoSqsMessage_ShouldSerializeBodyAndSetSubjectToTypeName()
    {
        var testMessage = new TestMessage { Id = Guid.NewGuid(), Name = Guid.NewGuid().ToString() };
        var metadata = new FifoMessageMetadata("Group", "Dedup", "Corr");

        var result = _factory.CreateFifoSqsMessage(TestSqsConstants.TestQueueUrl, testMessage, metadata);

        result.QueueUrl.Should().Be(TestSqsConstants.TestQueueUrl);
        result.MessageAttributes["Subject"].StringValue.Should().Be("Test");

        result.MessageBody.Should().Contain($"\"id\":\"{testMessage.Id}\"");
        result.MessageBody.Should().Contain($"\"name\":\"{testMessage.Name}\"");
    }

    [Fact]
    public void GivenCustomSubject_WhenCallingCreateFifoSqsMessage_ShouldUseProvidedSubject()
    {
        var testMessage = new TestMessage { Id = Guid.NewGuid(), Name = Guid.NewGuid().ToString() };
        var metadata = new FifoMessageMetadata("Group", "Dedup", "Corr");

        var result = _factory.CreateFifoSqsMessage(TestSqsConstants.TestQueueUrl, testMessage, metadata, subject: "CustomSubject");

        result.MessageAttributes["Subject"].StringValue.Should().Be("CustomSubject");
    }

    [Fact]
    public void GivenMetadata_WhenCallingCreateFifoSqsMessage_ShouldSetFifoFields()
    {
        var testMessage = new TestMessage { Id = Guid.NewGuid(), Name = "Test" };
        var metadata = new FifoMessageMetadata("Group-123", "Dedup-456", "Corr-789");

        var result = _factory.CreateFifoSqsMessage(TestSqsConstants.TestQueueUrl, testMessage, metadata);

        result.MessageGroupId.Should().Be("Group-123");
        result.MessageDeduplicationId.Should().Be("Dedup-456");
    }

    [Fact]
    public void GivenMetadataWithCorrelationId_WhenCallingCreateFifoSqsMessage_ShouldOverrideCorrelationId()
    {
        var testMessage = new TestMessage { Id = Guid.NewGuid(), Name = "Test" };
        var metadata = new FifoMessageMetadata("Group", "Dedup", "MyCorrelationId");

        var result = _factory.CreateFifoSqsMessage(TestSqsConstants.TestQueueUrl, testMessage, metadata);

        result.MessageAttributes["CorrelationId"].StringValue.Should().Be("MyCorrelationId");
    }

    [Fact]
    public void GivenMetadataWithAdditionalAttributes_WhenCallingCreateFifoSqsMessage_ShouldMergeAttributes()
    {
        var testMessage = new TestMessage { Id = Guid.NewGuid(), Name = "Test" };

        var metadata = new FifoMessageMetadata(
            "Group",
            "Dedup",
            "Corr",
            new Dictionary<string, string>
            {
                { "ExtraA", "ValueA" },
                { "ExtraB", "ValueB" }
            });

        var result = _factory.CreateFifoSqsMessage(TestSqsConstants.TestQueueUrl, testMessage, metadata);

        result.MessageAttributes["ExtraA"].StringValue.Should().Be("ValueA");
        result.MessageAttributes["ExtraB"].StringValue.Should().Be("ValueB");
    }

    [Fact]
    public void GivenMetadata_WhenCallingCreateFifoSqsMessage_ShouldStillIncludeDefaultAttributes()
    {
        var testMessage = new TestMessage { Id = Guid.NewGuid(), Name = "Test" };
        var metadata = new FifoMessageMetadata("Group", "Dedup", "Corr");

        var result = _factory.CreateFifoSqsMessage(TestSqsConstants.TestQueueUrl, testMessage, metadata);

        result.MessageAttributes.Should().ContainKey("EventTimeUtc");
        result.MessageAttributes.Should().ContainKey("Subject");
        result.MessageAttributes.Should().ContainKey("CorrelationId");
    }
}

public class TestMessage
{
    public Guid Id { get; set; }
    public string? Name { get; set; } = string.Empty;
}