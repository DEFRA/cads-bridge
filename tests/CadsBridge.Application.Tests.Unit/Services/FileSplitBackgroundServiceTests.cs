using System.Threading.Channels;
using CadsBridge.Application.Models;
using CadsBridge.Application.Persistance;
using CadsBridge.Application.Services;
using CadsBridge.Testing.Support.Utilities.Assertions;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Application.Tests.Unit.Services;

public class FileSplitBackgroundServiceTests : IAsyncDisposable
{
    private readonly Channel<FileSplitJob> _channel;
    private readonly Mock<ILogger<FileSplitBackgroundService>> _logger;
    private readonly Mock<ISplitJobProgressStore> _progressStore;
    private readonly Mock<IS3FileSplitterService> _s3FileSplitter;
    private readonly FileSplitBackgroundService _sut;

    private readonly FileSplitJob _job1 = CreateJob();

    public FileSplitBackgroundServiceTests()
    {
        _channel = Channel.CreateUnbounded<FileSplitJob>();
        _logger = new Mock<ILogger<FileSplitBackgroundService>>();
        _logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        _progressStore = new Mock<ISplitJobProgressStore>();
        _s3FileSplitter = new Mock<IS3FileSplitterService>();

        _s3FileSplitter
            .Setup(x => x.ExecuteAsync(
                It.IsAny<FileSplitJob>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        _sut = new FileSplitBackgroundService(
            _channel,
            _logger.Object,
            _progressStore.Object,
            _s3FileSplitter.Object);
    }

    [Fact]
    public async Task FileSplitBackgroundService_WhenJobIsReceived_MarksInProgressAndExecutesFileSplitterAndMarksSucceeded()
    {
        // Arrange
        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(_job1, TestContext.Current.CancellationToken);

        // Assert
        await _progressStore.AsyncVerify(x => x.MarkInProgress(_job1.JobId, _job1.Key), Times.Once);
        await _s3FileSplitter.AsyncVerify(x => x.ExecuteAsync(_job1, It.IsAny<CancellationToken>()), Times.Once);
        await _progressStore.AsyncVerify(x => x.MarkSucceeded(_job1.JobId, _job1.Key), Times.Once);
        _progressStore.Verify(x => x.MarkFailed(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task FileSplitBackgroundService_WhenFileSplitterReturnsFalse_MarksJobFailed()
    {
        // Arrange
        _s3FileSplitter
            .Setup(x => x.ExecuteAsync(_job1, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(_job1, TestContext.Current.CancellationToken);

        // Assert
        await _progressStore.AsyncVerify(x => x.MarkInProgress(_job1.JobId, _job1.Key), Times.Once);
        await _progressStore.AsyncVerify(x => x.MarkFailed(_job1.JobId, _job1.Key, It.IsAny<string>()), Times.Once);
        _progressStore.Verify(x => x.MarkSucceeded(It.IsAny<string>(), It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task FileSplitBackgroundService_WhenFileSplitterThrows_LogsExceptionAndMarksJobFailed()
    {
        // Arrange
        var exception = new InvalidOperationException("Split failed.");
        _s3FileSplitter
            .Setup(x => x.ExecuteAsync(_job1, It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);

        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(_job1, TestContext.Current.CancellationToken);

        // Assert
        await _s3FileSplitter.AsyncVerify(x => x.ExecuteAsync(_job1, It.IsAny<CancellationToken>()), Times.Once);
        await _progressStore.AsyncVerify(x => x.MarkFailed(_job1.JobId, _job1.Key, It.IsAny<string>()), Times.Once);
        await _logger.AsyncVerify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((value, _) =>
                    value.ToString()!.Contains($"Failed to split file {_job1.Key}")),
                exception,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task FileSplitBackgroundService_WhenMultipleJobsAreReceived_ExecutesFileSplitterForEachJob()
    {
        // Arrange
        var firstJob = CreateJob(1);
        var secondJob = CreateJob(2);
        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(firstJob, TestContext.Current.CancellationToken);
        await _channel.Writer.WriteAsync(secondJob, TestContext.Current.CancellationToken);

        // Assert
        await _s3FileSplitter.AsyncVerify(x => x.ExecuteAsync(It.IsAny<FileSplitJob>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        await _s3FileSplitter.AsyncVerify(x => x.ExecuteAsync(firstJob, It.IsAny<CancellationToken>()), Times.Once);
        await _s3FileSplitter.AsyncVerify(x => x.ExecuteAsync(secondJob, It.IsAny<CancellationToken>()), Times.Once);
        await _progressStore.AsyncVerify(x => x.MarkSucceeded(It.IsAny<string>(), It.IsAny<string>()), Times.Exactly(2));
    }

    private static FileSplitJob CreateJob(int jobNo = 1)
    {
        return new FileSplitJob(
            JobId: $"job-{jobNo}",
            Key: $"imported/file-{jobNo}.csv",
            TargetFolder: $"split-output/file-{jobNo}",
            SplitType: SplitType.ByLines,
            SplitValue: 100);
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.Complete();
        await _sut.StopAsync(TestContext.Current.CancellationToken);

        _sut.Dispose();
        GC.SuppressFinalize(this);
    }
}