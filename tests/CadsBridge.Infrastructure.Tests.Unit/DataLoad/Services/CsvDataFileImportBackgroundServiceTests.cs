using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Messaging;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Testing.Support.Utilities.Assertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Threading.Channels;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services;

public class CsvDataFileImportBackgroundServiceTests : IAsyncDisposable
{
    private readonly Channel<CsvDataFileImportJob> _channel;
    private readonly Mock<ILogger<CsvDataFileImportBackgroundService>> _logger;
    private readonly Mock<IImportJobProgressStore> _progressStore;
    private readonly Mock<ISplitMessageProducer> _splitMessageProducer;
    private readonly Mock<IS3CopyService> _fileImportCopyService;
    private readonly CsvDataFileImportBackgroundService _sut;

    private readonly CsvDataFileImportJob _job = CreateJob();

    public CsvDataFileImportBackgroundServiceTests()
    {
        _channel = Channel.CreateUnbounded<CsvDataFileImportJob>();
        _logger = new Mock<ILogger<CsvDataFileImportBackgroundService>>();
        _progressStore = new Mock<IImportJobProgressStore>();
        _splitMessageProducer = new Mock<ISplitMessageProducer>();
        _fileImportCopyService = new Mock<IS3CopyService>();

        _fileImportCopyService
            .Setup(x => x.ExecAsync(
                It.IsAny<CsvDataFileImportJob>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        _splitMessageProducer
            .Setup(x => x.SendAsync(
                It.IsAny<CsvDataFileSplitJob>(),
                It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        _sut = new CsvDataFileImportBackgroundService(
            _channel,
            _logger.Object,
            _progressStore.Object,
            _splitMessageProducer.Object,
            _fileImportCopyService.Object);
    }

    [Fact]
    public async Task FileImportBackgroundService_WhenJobIsReceived_MarksInProgressAndExecutesCopyService()
    {
        // Arrange
        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(_job, TestContext.Current.CancellationToken);

        // Assert
        await _progressStore.AsyncVerify(x => x.MarkInProgress(_job.JobId, _job.SourceKey), Times.Once);
        await _fileImportCopyService.AsyncVerify(x => x.ExecAsync(_job, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task FileImportBackgroundService_WhenCopySucceeds_MarksJobSucceeded()
    {
        // Arrange
        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(_job, TestContext.Current.CancellationToken);

        // Assert
        await _progressStore.AsyncVerify(x => x.MarkSucceeded(_job.JobId, _job.SourceKey), Times.Once);
        _progressStore.Verify(x => x.MarkFailed(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>()),
            Times.Never);
    }

    [Fact]
    public async Task FileImportBackgroundService_WhenCopySucceedsAndSplitTypeIsNone_DoesNotSendSplitMessage()
    {
        // Arrange
        var job = CreateJob(splitType: SplitType.None, splitValue: null);
        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(job, TestContext.Current.CancellationToken);

        // Assert
        await _progressStore.AsyncVerify(x => x.MarkSucceeded(job.JobId, job.SourceKey), Times.Once);

        _splitMessageProducer.Verify(x => x.SendAsync(
                It.IsAny<CsvDataFileSplitJob>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task FileImportBackgroundService_WhenCopySucceedsAndSplitTypeIsNotNone_SendsSplitMessage()
    {
        // Arrange
        var job = CreateJob(
            targetKey: "imported/example-file.csv",
            splitType: SplitType.ByLines,
            splitValue: 10);

        var expectedSplitJob = new CsvDataFileSplitJob(
            JobId: job.JobId,
            Key: job.TargetKey,
            TargetFolder: "example-file",
            SplitType: SplitType.ByLines,
            SplitValue: 10);

        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(job, TestContext.Current.CancellationToken);

        // Assert
        await _splitMessageProducer.AsyncVerify(x => x.SendAsync(expectedSplitJob, It.IsAny<CancellationToken>()), Times.Once);
        await _progressStore.AsyncVerify(x => x.MarkSucceeded(job.JobId, job.SourceKey), Times.Once);
    }

    [Fact]
    public async Task FileImportBackgroundService_WhenCopyServiceReturnsFalse_MarksJobFailed()
    {
        // Arrange
        _fileImportCopyService
            .Setup(x => x.ExecAsync(_job, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(_job, TestContext.Current.CancellationToken);

        // Assert
        await _progressStore.AsyncVerify(x => x.MarkInProgress(_job.JobId, _job.SourceKey), Times.Once);

        await _progressStore.AsyncVerify(
            x => x.MarkFailed(
                _job.JobId,
                _job.SourceKey,
                "Unknown error during copy"),
            Times.Once);

        _progressStore.Verify(
            x => x.MarkSucceeded(
                It.IsAny<string>(),
                It.IsAny<string>()),
            Times.Never);

        _splitMessageProducer.Verify(
            x => x.SendAsync(
                It.IsAny<CsvDataFileSplitJob>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task FileImportBackgroundService_WhenCopyServiceThrows_LogsExceptionAndMarksJobFailed()
    {
        // Arrange
        var exception = new InvalidOperationException("Copy failed.");

        _fileImportCopyService
            .Setup(x => x.ExecAsync(_job, It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);

        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(_job, TestContext.Current.CancellationToken);

        // Assert
        await _progressStore.AsyncVerify(x => x.MarkInProgress(_job.JobId, _job.SourceKey), Times.Once);

        await _progressStore.AsyncVerify(
            x => x.MarkFailed(
                _job.JobId,
                _job.SourceKey,
                exception.Message),
            Times.Once);

        await _logger.AsyncVerify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((value, _) =>
                    value.ToString()!.Contains($"Failed to import {_job.SourceKey}")),
                exception,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);

        _splitMessageProducer.Verify(
            x => x.SendAsync(
                It.IsAny<CsvDataFileSplitJob>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task FileImportBackgroundService_WhenSplitMessageProducerThrows_LogsExceptionAndMarksJobFailed()
    {
        // Arrange
        var job = CreateJob(
            splitType: SplitType.ByLines,
            splitValue: 10);

        var exception = new InvalidOperationException("Split message failed.");

        _splitMessageProducer
            .Setup(x => x.SendAsync(
                It.IsAny<CsvDataFileSplitJob>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);

        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(job, TestContext.Current.CancellationToken);

        // Assert
        await _progressStore.AsyncVerify(x => x.MarkSucceeded(job.JobId, job.SourceKey), Times.Once);

        await _progressStore.AsyncVerify(
            x => x.MarkFailed(
                job.JobId,
                job.SourceKey,
                exception.Message),
            Times.Once);

        await _logger.AsyncVerify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((value, _) =>
                    value.ToString()!.Contains($"Failed to import {job.SourceKey}")),
                exception,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task FileImportBackgroundService_WhenMultipleJobsAreReceived_ExecutesCopyServiceForEachJob()
    {
        // Arrange
        var firstJob = CreateJob(jobNo: 1);
        var secondJob = CreateJob(jobNo: 2);

        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(firstJob, TestContext.Current.CancellationToken);
        await _channel.Writer.WriteAsync(secondJob, TestContext.Current.CancellationToken);

        // Assert
        await _fileImportCopyService.AsyncVerify(
            x => x.ExecAsync(
                It.IsAny<CsvDataFileImportJob>(),
                It.IsAny<CancellationToken>()),
            Times.Exactly(2));

        await _fileImportCopyService.AsyncVerify(x => x.ExecAsync(firstJob, It.IsAny<CancellationToken>()), Times.Once);
        await _fileImportCopyService.AsyncVerify(x => x.ExecAsync(secondJob, It.IsAny<CancellationToken>()), Times.Once);

        await _progressStore.AsyncVerify(
            x => x.MarkSucceeded(
                It.IsAny<string>(),
                It.IsAny<string>()),
            Times.Exactly(2));
    }

    [Fact]
    public async Task FileImportBackgroundService_WhenTargetKeyHasNoExtension_UsesTargetKeyAsSplitTargetFolder()
    {
        // Arrange
        var job = CreateJob(
            targetKey: "imported/example-file",
            splitType: SplitType.BySize,
            splitValue: 25);

        var expectedSplitJob = new CsvDataFileSplitJob(
            JobId: job.JobId,
            Key: job.TargetKey,
            TargetFolder: "example-file",
            SplitType: SplitType.BySize,
            SplitValue: 25);

        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(job, TestContext.Current.CancellationToken);

        // Assert
        await _splitMessageProducer.AsyncVerify(
            x => x.SendAsync(expectedSplitJob, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    private static CsvDataFileImportJob CreateJob(
        int jobNo = 1,
        string? sourceKey = null,
        string? targetKey = null,
        SplitType splitType = SplitType.None,
        int? splitValue = null)
    {
        return new CsvDataFileImportJob(
            JobId: $"job-{jobNo}",
            SourceKey: sourceKey ?? $"incoming/file-{jobNo}.csv",
            TargetKey: targetKey ?? $"imported/file-{jobNo}.csv",
            Password: "test-password",
            Salt: "test-salt",
            SplitType: splitType,
            SplitValue: splitValue);
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.Complete();
        await _sut.StopAsync(TestContext.Current.CancellationToken);
        _sut.Dispose();
        GC.SuppressFinalize(this);
    }
}