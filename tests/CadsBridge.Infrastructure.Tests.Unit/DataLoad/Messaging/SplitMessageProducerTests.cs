using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.DataLoad.Messaging;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Threading.Channels;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Messaging;

public class SplitMessageProducerTests
{
    private readonly Channel<CsvDataFileSplitJob> _channel;
    private readonly SplitMessageProducer _sut;

    public SplitMessageProducerTests()
    {
        _channel = Channel.CreateUnbounded<CsvDataFileSplitJob>();
        var loggerMock = new Mock<ILogger<SplitMessageProducer>>();
        loggerMock.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        _sut = new SplitMessageProducer(_channel, loggerMock.Object);
    }

    [Fact]
    public async Task SendAsync_ShouldAddJobToChannel()
    {
        var fileSplitJob = new CsvDataFileSplitJob("job-1", "key", "target-folder", SplitType.ByLines, 100);

        await _sut.SendAsync(fileSplitJob, TestContext.Current.CancellationToken);
        _channel.Writer.Complete();

        var results = await _channel.Reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
        results.Should().BeEquivalentTo([fileSplitJob]);
    }
}