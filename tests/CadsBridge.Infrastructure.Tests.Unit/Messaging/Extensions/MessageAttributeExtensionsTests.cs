using Amazon.SQS.Model;
using CadsBridge.Infrastructure.Messaging.Extensions;
using FluentAssertions;

namespace CadsBridge.Infrastructure.Tests.Unit.Messaging.Extensions;

public class MessageAttributeExtensionsTests
{
    [Fact]
    public void GetMessageAttributeValue_FromSqsMessage_ShouldReturnNull_WhenMessageAttributesIsNull()
    {
        var message = new Message();
        var result = message.GetMessageAttributeValue<string>("MissingKey");
        result.Should().BeNull();
    }

    [Fact]
    public void GetMessageAttributeValue_FromSqsMessage_ShouldReturnNull_WhenKeyMissing()
    {
        var message = new Message { MessageAttributes = [] };
        var result = message.GetMessageAttributeValue<string>("MissingKey");
        result.Should().BeNull();
    }

    [Fact]
    public void GetMessageAttributeValue_FromSqsMessage_ShouldReturnStringValue()
    {
        var message = BuildSqsEnvelope("String", "Subject", "PlaceholderMessage");
        var result = message.GetMessageAttributeValue<string>("Subject");
        result.Should().Be("PlaceholderMessage");
    }

    [Fact]
    public void GetMessageAttributeValue_FromSqsMessage_ShouldReturnIntValue()
    {
        var message = BuildSqsEnvelope("Number", "RetryCount", "3");
        var result = message.GetMessageAttributeValue<int>("RetryCount");
        result.Should().Be(3);
    }

    [Fact]
    public void GetMessageAttributeValue_FromSqsMessage_ShouldReturnDoubleValue()
    {
        var message = BuildSqsEnvelope("Number", "Confidence", "0.85");
        var result = message.GetMessageAttributeValue<double>("Confidence");
        result.Should().BeApproximately(0.85, 0.0001);
    }

    [Fact]
    public void GetMessageAttributeValue_FromSqsMessage_ShouldReturnDefault_WhenTypeMismatch()
    {
        var message = BuildSqsEnvelope("String", "Subject", "PlaceholderMessage");
        var result = message.GetMessageAttributeValue<int>("Subject");
        result.Should().Be(0);
    }

    [Fact]
    public void GetMessageAttributeValue_FromSqsMessage_ShouldReturnDefault_WhenValueIsInvalid()
    {
        var message = BuildSqsEnvelope("Number", "RetryCount", "not-an-int");
        var result = message.GetMessageAttributeValue<int>("RetryCount");
        result.Should().Be(0);
    }

    [Theory]
    [InlineData("PlaceholderMessage", "Placeholder")]
    [InlineData("UserCreatedMessage", "UserCreated")]
    [InlineData("Message", "")]
    [InlineData("NoSuffix", "NoSuffix")]
    [InlineData(null, "")]
    public void ReplaceSuffix_ShouldStripMessageSuffix(string? input, string expected)
    {
        var result = input.ReplaceSuffix();
        result.Should().Be(expected);
    }

    private static Message BuildSqsEnvelope(string dataType, string key, string value)
    {
        return new Message
        {
            MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                [key] = new MessageAttributeValue
                {
                    DataType = dataType,
                    StringValue = value
                }
            }
        };
    }
}