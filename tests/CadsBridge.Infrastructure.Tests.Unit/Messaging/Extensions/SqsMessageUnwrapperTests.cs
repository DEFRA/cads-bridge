using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Models;
using CadsBridge.Infrastructure.Messaging.Extensions;
using FluentAssertions;

namespace CadsBridge.Infrastructure.Tests.Unit.Messaging.Extensions;

public class SqsMessageUnwrapperTests
{
    private const string Payload = "{\"id\":\"00000000-0000-0000-0000-000000000001\", \"message\":\"Test message 1\"}";
    private const string SubjectKey = "Subject";
    private const string CorrelationIdKey = "CorrelationId";

    [Fact]
    public void GivenNullMessage_WhenCallingUnwrap_ShouldThrowArgumentNullException()
    {
        Message? message = null;

        var act = () => message!.Unwrap();

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GivenRawMessageWithMissingAttributes_ShouldUseDefaults()
    {
        var message = new Message
        {
            MessageId = "raw-id",
            Body = Payload,
            MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                [SubjectKey] = new() { DataType = "String" },
                [CorrelationIdKey] = new() { DataType = "String", StringValue = null }
            }
        };

        var result = message.Unwrap();

        result.Subject.Should().Be("Default");
        result.CorrelationId.Should().Be(string.Empty);
    }

    [Fact]
    public void GivenRawMessage_WhenCallingUnwrap_ShouldMapAllFieldsCorrectly()
    {
        var messageId = Guid.NewGuid().ToString();
        var correlationId = Guid.NewGuid().ToString();

        var message = BuildSqsMessage(messageId, correlationId, "PlaceholderMessage");
        var result = message.Unwrap();

        VerifyUnwrappedMessage(messageId, correlationId, "Placeholder", Payload, result);
    }

    [Fact]
    public void GivenMessageWithNoSubjectAttribute_ShouldDefaultSubject()
    {
        var message = new Message
        {
            MessageId = "123",
            Body = Payload,
            MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                [CorrelationIdKey] = new() { DataType = "String", StringValue = "ABC" }
            }
        };

        var result = message.Unwrap();

        result.Subject.Should().Be("Default");
    }

    [Fact]
    public void GivenMessageWithNoCorrelationIdAttribute_ShouldDefaultCorrelationId()
    {
        var message = new Message
        {
            MessageId = "123",
            Body = Payload,
            MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                [SubjectKey] = new() { DataType = "String", StringValue = "TestSubject" }
            }
        };

        var result = message.Unwrap();

        result.CorrelationId.Should().Be(string.Empty);
    }

    [Fact]
    public void GivenMessageWithEmptyBody_ShouldSetPayloadToEmptyString()
    {
        var message = new Message
        {
            MessageId = "123",
            Body = null,
            MessageAttributes = []
        };

        var result = message.Unwrap();

        result.Payload.Should().Be(string.Empty);
    }

    [Fact]
    public void GivenMessageWithAttributes_ShouldMapAttributesDictionaryCorrectly()
    {
        var message = new Message
        {
            MessageId = "123",
            Body = Payload,
            MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                ["A"] = new() { DataType = "String", StringValue = "ValueA" },
                ["B"] = new() { DataType = "String", StringValue = "ValueB" },
                ["C"] = new() { DataType = "String", StringValue = null }
            }
        };

        var result = message.Unwrap();

        result.Attributes.Should().ContainKey("A");
        result.Attributes["A"].Should().Be("ValueA");

        result.Attributes.Should().ContainKey("B");
        result.Attributes["B"].Should().Be("ValueB");

        result.Attributes.Should().ContainKey("C");
        result.Attributes["C"].Should().Be(string.Empty);
    }

    [Fact]
    public void GivenMessageWithSuffixInSubject_ShouldReplaceSuffix()
    {
        var message = BuildSqsMessage("123", "ABC", "MySubjectMessage");

        var result = message.Unwrap();

        result.Subject.Should().Be("MySubject"); // ReplaceSuffix() behaviour
    }

    private static Message BuildSqsMessage(string messageId, string correlationId, string subject)
    {
        return new Message
        {
            MessageId = messageId,
            Body = Payload,
            MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                [SubjectKey] = new() { DataType = "String", StringValue = subject },
                [CorrelationIdKey] = new() { DataType = "String", StringValue = correlationId }
            }
        };
    }

    private static void VerifyUnwrappedMessage(
        string expectedId,
        string expectedCorrelationId,
        string expectedSubject,
        string expectedPayload,
        UnwrappedMessage actual)
    {
        actual.MessageId.Should().Be(expectedId);
        actual.CorrelationId.Should().Be(expectedCorrelationId);
        actual.Subject.Should().Be(expectedSubject);
        actual.Payload.Should().Be(expectedPayload);

        actual.Attributes.Should().ContainKeys(SubjectKey, CorrelationIdKey);
    }
}