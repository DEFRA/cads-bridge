using System.Threading.Channels;
using CadsBridge.Application.Models;
using CadsBridge.Infrastructure.FileSplit;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.FileSplit;

public class SplitMessageProducerTests
{
    private readonly Channel<FileSplitJob> _channel;
    private readonly SplitMessageProducer _sut;

    public SplitMessageProducerTests()
    {
        _channel = Channel.CreateUnbounded<FileSplitJob>();
        var loggerMock = new Mock<ILogger<SplitMessageProducer>>();
        loggerMock.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        _sut = new SplitMessageProducer(_channel, loggerMock.Object);
    }

    [Fact]
    public async Task SendAsync_ShouldAddJobToChannel()
    {
        var fileSplitJob = new FileSplitJob("job-1", "key", "target-folder", SplitType.ByLines, 100);

        await _sut.SendAsync(fileSplitJob, TestContext.Current.CancellationToken);
        _channel.Writer.Complete();

        var results = await _channel.Reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
        results.Should().BeEquivalentTo([fileSplitJob]);
    }
}