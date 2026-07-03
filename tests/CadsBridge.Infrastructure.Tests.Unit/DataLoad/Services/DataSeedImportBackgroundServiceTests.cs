using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Testing.Support.Utilities.Assertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Threading.Channels;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services;

public class DataSeedImportBackgroundServiceTests : IAsyncDisposable
{
    private readonly Channel<DataSeedFileLoadJob> _channel = Channel.CreateUnbounded<DataSeedFileLoadJob>();
    private readonly Mock<IFileSystemToS3CopyService> _copy = new();
    private readonly Mock<ILogger<DataSeedImportBackgroundService>> _logger = new();
    private readonly DataSeedImportBackgroundService _sut;

    public DataSeedImportBackgroundServiceTests()
    {
        _logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        _copy.Setup(x => x.ExecuteAsync(It.IsAny<DataSeedFileLoadJob>(), It.IsAny<CancellationToken>()))
             .ReturnsAsync(true);

        _sut = new DataSeedImportBackgroundService(_channel, _copy.Object, _logger.Object);
    }

    [Fact]
    public async Task Executes_copy_service_when_job_received()
    {
        await _sut.StartAsync(CancellationToken.None);

        var job = CreateJob(1);
        await Write(job);

        await _copy.AsyncVerify(x => x.ExecuteAsync(job, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Logs_failure_when_copy_service_returns_false()
    {
        var job = CreateJob(1);

        _copy.Setup(x => x.ExecuteAsync(job, It.IsAny<CancellationToken>()))
             .ReturnsAsync(false);

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _copy.AsyncVerify(x => x.ExecuteAsync(job, It.IsAny<CancellationToken>()), Times.Once);

        await _logger.AsyncVerify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains("Failed")),
                null,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task Logs_exception_when_copy_service_throws()
    {
        var job = CreateJob(1);
        var ex = new InvalidOperationException("Copy failed");

        _copy.Setup(x => x.ExecuteAsync(job, It.IsAny<CancellationToken>()))
             .ThrowsAsync(ex);

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _copy.AsyncVerify(x => x.ExecuteAsync(job, It.IsAny<CancellationToken>()), Times.Once);

        await _logger.AsyncVerify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains("Failed")),
                ex,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task Executes_copy_service_for_each_job()
    {
        var job1 = CreateJob(1);
        var job2 = CreateJob(2);

        await _sut.StartAsync(CancellationToken.None);

        await Write(job1);
        await Write(job2);

        await _copy.AsyncVerify(x => x.ExecuteAsync(It.IsAny<DataSeedFileLoadJob>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        await _copy.AsyncVerify(x => x.ExecuteAsync(job1, It.IsAny<CancellationToken>()), Times.Once);
        await _copy.AsyncVerify(x => x.ExecuteAsync(job2, It.IsAny<CancellationToken>()), Times.Once);
    }

    private Task Write(DataSeedFileLoadJob job) =>
        _channel.Writer.WriteAsync(job).AsTask();

    private static DataSeedFileLoadJob CreateJob(int n) =>
        new($"job-{n}", $"00{n}_seed.sql", $"data-seed/00{n}_seed.sql");

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.Complete();
        await _sut.StopAsync(CancellationToken.None);
        _sut.Dispose();
        GC.SuppressFinalize(this);
    }
}