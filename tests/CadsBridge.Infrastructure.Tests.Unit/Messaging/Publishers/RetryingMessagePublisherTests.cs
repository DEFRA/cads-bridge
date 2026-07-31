using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Models;
using CadsBridge.Application.Messaging.Publishers;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Messaging.Publishers;
using FluentAssertions;
using Moq;
using Polly;

namespace CadsBridge.Infrastructure.Tests.Unit.Messaging.Publishers;

public class RetryingMessagePublisherTests
{
    private readonly Mock<IMessagePublisher<CadsBridgeFifoQueueClient>> _innerMock = new();
    private static readonly FifoMessageMetadata Metadata = new("Group", "Dedup", "CorrelationId");

    private static ResiliencePipeline BuildRetryPipeline(int maxRetryAttempts = 2) =>
        new ResiliencePipelineBuilder()
            .AddRetry(new Polly.Retry.RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder().Handle<PublishFailedException>(ex => ex.IsTransient),
                MaxRetryAttempts = maxRetryAttempts,
                Delay = TimeSpan.Zero,
                UseJitter = false
            })
            .Build();

    private RetryingMessagePublisher<CadsBridgeFifoQueueClient> CreateSut(ResiliencePipeline? pipeline = null) =>
        new(_innerMock.Object, pipeline ?? BuildRetryPipeline());

    [Fact]
    public void QueueUrl_ShouldReturnInnerPublisherQueueUrl()
    {
        _innerMock.Setup(x => x.QueueUrl).Returns("https://example.com/queue");
        var sut = CreateSut();

        sut.QueueUrl.Should().Be("https://example.com/queue");
    }

    [Fact]
    public async Task PublishAsync_ShouldCallInnerPublisher_Once_WhenSuccessful()
    {
        var message = new { Content = "hello" };
        _innerMock
            .Setup(x => x.PublishAsync(message, Metadata, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var sut = CreateSut();
        await sut.PublishAsync(message, Metadata, TestContext.Current.CancellationToken);

        _innerMock.Verify(x => x.PublishAsync(message, Metadata, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task PublishAsync_ShouldRetry_WhenInnerPublisherThrowsTransientPublishFailedException()
    {
        var message = new { Content = "hello" };
        var attempts = 0;

        _innerMock
            .Setup(x => x.PublishAsync(message, Metadata, It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                attempts++;
                if (attempts < 3)
                {
                    throw new PublishFailedException("transient failure", isTransient: true);
                }
                return Task.CompletedTask;
            });

        var sut = CreateSut(BuildRetryPipeline(maxRetryAttempts: 3));
        await sut.PublishAsync(message, Metadata, TestContext.Current.CancellationToken);

        attempts.Should().Be(3); // 1 initial attempt + 2 retries before succeeding
    }

    [Fact]
    public async Task PublishAsync_ShouldNotRetry_WhenInnerPublisherThrowsNonTransientPublishFailedException()
    {
        var message = new { Content = "hello" };
        var attempts = 0;

        _innerMock
            .Setup(x => x.PublishAsync(message, Metadata, It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                attempts++;
                throw new PublishFailedException("permanent failure", isTransient: false);
            });

        var sut = CreateSut();
        var act = async () => await sut.PublishAsync(message, Metadata, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<PublishFailedException>();
        attempts.Should().Be(1);
    }

    [Fact]
    public async Task PublishAsync_ShouldThrowAfterExhaustingRetries_WhenAlwaysTransient()
    {
        var message = new { Content = "hello" };
        var attempts = 0;

        _innerMock
            .Setup(x => x.PublishAsync(message, Metadata, It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                attempts++;
                throw new PublishFailedException("always transient", isTransient: true);
            });

        var sut = CreateSut(BuildRetryPipeline(maxRetryAttempts: 2));
        var act = async () => await sut.PublishAsync(message, Metadata, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<PublishFailedException>();
        attempts.Should().Be(3); // 1 initial attempt + 2 retries, all failing
    }
}

