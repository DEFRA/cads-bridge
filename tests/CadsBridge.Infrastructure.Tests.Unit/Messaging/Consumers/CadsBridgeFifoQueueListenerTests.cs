using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Consumers;
using CadsBridge.Infrastructure.Messaging.Consumers;
using CadsBridge.Testing.Support.Utilities.Logging;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.Messaging.Consumers;

public class CadsBridgeFifoQueueListenerTests
{
    private readonly Mock<IQueuePoller<CadsBridgeFifoQueueClient>> _queuePollerMock = new();
    private readonly Mock<ILogger<CadsBridgeFifoQueueListener>> _loggerMock =
        new Mock<ILogger<CadsBridgeFifoQueueListener>>().EnableAllLogLevels();

    private static IConfiguration BuildConfiguration(bool? disableQueueConsumer = null)
    {
        var values = new Dictionary<string, string?>();
        if (disableQueueConsumer.HasValue)
        {
            values["Messaging:DisableQueueConsumer"] = disableQueueConsumer.Value.ToString();
        }

        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    private CadsBridgeFifoQueueListener CreateSut(bool? disableQueueConsumer = null) =>
        new(_queuePollerMock.Object, BuildConfiguration(disableQueueConsumer), _loggerMock.Object);

    [Fact]
    public async Task StartAsync_ShouldStartPoller_WhenNotDisabled()
    {
        var sut = CreateSut(disableQueueConsumer: false);

        await sut.StartAsync(TestContext.Current.CancellationToken);

        _queuePollerMock.Verify(x => x.StartAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StartAsync_ShouldStartPoller_WhenConfigurationNotSet()
    {
        var sut = CreateSut();

        await sut.StartAsync(TestContext.Current.CancellationToken);

        _queuePollerMock.Verify(x => x.StartAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StartAsync_ShouldSkipPoller_WhenDisabled()
    {
        var sut = CreateSut(disableQueueConsumer: true);

        await sut.StartAsync(TestContext.Current.CancellationToken);

        _queuePollerMock.Verify(x => x.StartAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task StopAsync_ShouldStopPoller_WhenNotDisabled()
    {
        var sut = CreateSut(disableQueueConsumer: false);

        await sut.StopAsync(TestContext.Current.CancellationToken);

        _queuePollerMock.Verify(x => x.StopAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StopAsync_ShouldSkipPoller_WhenDisabled()
    {
        var sut = CreateSut(disableQueueConsumer: true);

        await sut.StopAsync(TestContext.Current.CancellationToken);

        _queuePollerMock.Verify(x => x.StopAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task StopAsync_ShouldSwallowTaskCanceledException_FromPoller()
    {
        _queuePollerMock
            .Setup(x => x.StopAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TaskCanceledException());

        var sut = CreateSut(disableQueueConsumer: false);

        var act = async () => await sut.StopAsync(TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task StopAsync_ShouldSwallowObjectDisposedException_FromPoller()
    {
        _queuePollerMock
            .Setup(x => x.StopAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ObjectDisposedException("poller"));

        var sut = CreateSut(disableQueueConsumer: false);

        var act = async () => await sut.StopAsync(TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync();
    }
}


