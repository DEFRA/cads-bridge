using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using CadsBridge.Infrastructure.DataLoad.Messaging;
using CadsBridge.Testing.Support.Utilities.Logging;
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

        var config = new DataLoadConfiguration { SplitType = SplitType.ByLines, SplitValue = 100 };

        var logger = new Mock<ILogger<SplitMessageProducer>>().EnableAllLogLevels();

        _sut = new SplitMessageProducer(_channel, config, logger.Object);
    }

    [Fact]
    public async Task SendAsync_writes_message_to_channel()
    {
        var job = new CsvDataFileSplitJob(
            SourceKey: "key");

        await _sut.SendAsync(job, CancellationToken.None);

        _channel.Writer.Complete();

        var messages = await _channel.Reader
            .ReadAllAsync(TestContext.Current.CancellationToken)
            .ToListAsync(cancellationToken: TestContext.Current.CancellationToken);

        messages.Should().ContainSingle()
                .Which.Should().BeEquivalentTo(job);
    }
}