using Amazon.SQS;
using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Models;
using CadsBridge.Core.Correlation;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Messaging.Configuration;
using CadsBridge.Infrastructure.Messaging.Factories;
using CadsBridge.Infrastructure.Messaging.Publishers;
using CadsBridge.Testing.Support.Constants;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.Messaging.Publishers;

public class MessagePublisherTests
{
    private readonly Mock<IAmazonSQS> _sqsMock = new();
    private readonly Mock<IMessageFactory> _messageFactoryMock = new();
    private readonly Mock<IOptionsMonitor<QueuePublisherOptions>> _optionsMonitorMock = new();

    private readonly QueuePublisherOptions _queuePublisherOptions = new()
    {
        Name = "TestQueue",
        QueueUrl = TestSqsConstants.TestQueueUrl
    };

    private readonly CadsBridgeFifoQueueClient _cadsBridgeFifoQueueClient = new();

    private CadsBridgeFifoQueuePublisher CreateSut()
    {
        _optionsMonitorMock
            .Setup(x => x.Get(It.IsAny<string>()))
            .Returns(_queuePublisherOptions);

        return new CadsBridgeFifoQueuePublisher(
            _sqsMock.Object,
            _messageFactoryMock.Object,
            _optionsMonitorMock.Object,
            _cadsBridgeFifoQueueClient);
    }

    [Fact]
    public void QueueUrl_ShouldReturnConfigurationValue_WhenCalled()
    {
        var sut = CreateSut();

        var result = sut.QueueUrl;

        result.Should().Be(TestSqsConstants.TestQueueUrl);
    }

    [Fact]
    public async Task PublishAsync_ShouldThrowArgumentException_WhenMessageIsNull()
    {
        var sut = CreateSut();
        object? message = null;

        var metadata = new FifoMessageMetadata("Group", "Dedup", "MyCorrelationId");
        var action = () => sut.PublishAsync(message, metadata);

        await action.Should().ThrowAsync<ArgumentException>()
            .WithMessage("Message payload was null (Parameter 'message')"); ;
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task PublishAsync_ShouldThrowPublishFailedException_WhenQueueUrlIsEmpty(string? queueUrl)
    {
        var message = new { Content = "Test message" };
        var sut = CreateSut();

        _optionsMonitorMock
            .Setup(x => x.Get(It.IsAny<string>()))
            .Returns(new QueuePublisherOptions { Name = "TestQueue", QueueUrl = queueUrl! });

        var metadata = new FifoMessageMetadata("Group", "Dedup", "MyCorrelationId");
        var action = () => sut.PublishAsync(message, metadata);

        await action.Should().ThrowAsync<PublishFailedException>()
            .WithMessage("QueueUrl is missing");
    }

    [Fact]
    public async Task PublishAsync_ShouldThrowPublishFailedException_WhenSqsSendFails()
    {
        var message = new { Content = "Test message" };
        var sendRequest = new SendMessageRequest { QueueUrl = TestSqsConstants.TestQueueUrl, MessageBody = "serialized message" };
        var metadata = new FifoMessageMetadata("Group", "Dedup", "MyCorrelationId");
        var innerException = new Exception("SQS service unavailable");

        var sut = CreateSut();

        _messageFactoryMock
            .Setup(x => x.CreateFifoSqsMessage(TestSqsConstants.TestQueueUrl, message, metadata, It.IsAny<string?>()))
            .Returns(sendRequest);

        _sqsMock
            .Setup(x => x.SendMessageAsync(sendRequest, It.IsAny<CancellationToken>()))
            .ThrowsAsync(innerException);

        var action = () => sut.PublishAsync(message, metadata, TestContext.Current.CancellationToken);

        await action.Should().ThrowAsync<PublishFailedException>()
            .WithMessage($"Failed to publish message on {TestSqsConstants.TestQueueUrl}.");

        // Clean up
        CorrelationIdContext.Value = null;
    }

    [Fact]
    public async Task PublishAsync_ShouldSendMessage_WhenValidMessageProvided()
    {
        var correlationId = "test-correlation-id";
        var message = new { Content = "Test message" };
        var sendRequest = new SendMessageRequest
        {
            QueueUrl = TestSqsConstants.TestQueueUrl,
            MessageBody = "serialized message"
        };

        var metadata = new FifoMessageMetadata(
            "Group",
            "Dedup",
            "Corr",
            new Dictionary<string, string>
            {
                { "ExtraA", "ValueA" },
                { "ExtraB", "ValueB" }
            });

        var sut = CreateSut();

        // Set up the correlation ID context
        CorrelationIdContext.Value = correlationId;

        var expectedAttributes = new Dictionary<string, string>
        {
            ["CorrelationId"] = correlationId
        };

        _messageFactoryMock
            .Setup(x => x.CreateFifoSqsMessage(TestSqsConstants.TestQueueUrl, message, metadata))
            .Returns(sendRequest);

        _sqsMock
            .Setup(x => x.SendMessageAsync(sendRequest, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new SendMessageResponse { MessageId = "test-message-id" });

        await sut.PublishAsync(message, metadata, TestContext.Current.CancellationToken);

        _messageFactoryMock.Verify(x => x.CreateFifoSqsMessage(TestSqsConstants.TestQueueUrl, message, metadata, subject: null), Times.Once);
        _sqsMock.Verify(x => x.SendMessageAsync(sendRequest, It.IsAny<CancellationToken>()), Times.Once);

        // Clean up
        CorrelationIdContext.Value = null;
    }
}