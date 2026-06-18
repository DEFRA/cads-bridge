using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Messaging;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Testing.Support.Utilities.Assertions;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Threading.Channels;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services;

public class CsvDataFileImportBackgroundServiceTests : IAsyncDisposable
{
    private readonly Channel<CsvDataFileImportJob> _channel = Channel.CreateUnbounded<CsvDataFileImportJob>();
    private readonly Mock<ILogger<CsvDataFileImportBackgroundService>> _logger = new();
    private readonly Mock<IImportJobProgressStore> _progress = new();
    private readonly Mock<ISplitMessageProducer> _splitProducer = new();
    private readonly Mock<IS3CopyService> _copy = new();
    private readonly CsvDataFileImportBackgroundService _sut;

    public CsvDataFileImportBackgroundServiceTests()
    {
        _logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);

        _copy.Setup(x => x.ExecAsync(It.IsAny<CsvDataFileImportJob>(), It.IsAny<CancellationToken>()))
             .ReturnsAsync(true);

        _splitProducer.Setup(x => x.SendAsync(It.IsAny<CsvDataFileSplitJob>(), It.IsAny<CancellationToken>()))
                      .Returns(ValueTask.CompletedTask);

        _sut = new CsvDataFileImportBackgroundService(
            _channel,
            _logger.Object,
            _progress.Object,
            _splitProducer.Object,
            _copy.Object);
    }

    [Fact]
    public async Task Marks_in_progress_and_executes_copy()
    {
        await _sut.StartAsync(CancellationToken.None);

        var job = CreateJob();
        await Write(job);

        await _progress.AsyncVerify(x => x.MarkInProgress(job.JobId, job.SourceKey), Times.Once);
        await _copy.AsyncVerify(x => x.ExecAsync(job, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Marks_succeeded_when_copy_succeeds()
    {
        await _sut.StartAsync(CancellationToken.None);

        var job = CreateJob();
        await Write(job);

        await _progress.AsyncVerify(x => x.MarkSucceeded(job.JobId, job.SourceKey), Times.Once);
        _progress.Verify(x => x.MarkFailed(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task Does_not_send_split_message_when_split_type_none()
    {
        var job = CreateJob(splitType: SplitType.None, splitValue: null);

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _progress.AsyncVerify(x => x.MarkSucceeded(job.JobId, job.SourceKey), Times.Once);

        _splitProducer.Verify(x => x.SendAsync(It.IsAny<CsvDataFileSplitJob>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Sends_split_message_when_split_type_specified()
    {
        var job = CreateJob(
            targetKey: "imported/example-file.csv",
            splitType: SplitType.ByLines,
            splitValue: 10);

        var expected = new CsvDataFileSplitJob(
            JobId: job.JobId,
            Key: job.TargetKey,
            TargetFolder: "example-file",
            SplitType: SplitType.ByLines,
            SplitValue: 10);

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _splitProducer.AsyncVerify(x => x.SendAsync(expected, It.IsAny<CancellationToken>()), Times.Once);
        await _progress.AsyncVerify(x => x.MarkSucceeded(job.JobId, job.SourceKey), Times.Once);
    }

    [Fact]
    public async Task Marks_failed_when_copy_returns_false()
    {
        var job = CreateJob();

        _copy.Setup(x => x.ExecAsync(job, It.IsAny<CancellationToken>()))
             .ReturnsAsync(false);

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _progress.AsyncVerify(x => x.MarkInProgress(job.JobId, job.SourceKey), Times.Once);

        await _progress.AsyncVerify(
            x => x.MarkFailed(job.JobId, job.SourceKey, "Unknown error during copy"),
            Times.Once);

        _progress.Verify(x => x.MarkSucceeded(It.IsAny<string>(), It.IsAny<string>()), Times.Never);
        _splitProducer.Verify(x => x.SendAsync(It.IsAny<CsvDataFileSplitJob>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Logs_exception_and_marks_failed_when_copy_throws()
    {
        var job = CreateJob();
        var ex = new InvalidOperationException("Copy failed.");

        _copy.Setup(x => x.ExecAsync(job, It.IsAny<CancellationToken>()))
             .ThrowsAsync(ex);

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _progress.AsyncVerify(x => x.MarkInProgress(job.JobId, job.SourceKey), Times.Once);
        await _progress.AsyncVerify(x => x.MarkFailed(job.JobId, job.SourceKey, ex.Message), Times.Once);

        await _logger.AsyncVerify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains($"Failed to import {job.SourceKey}")),
                ex,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);

        _splitProducer.Verify(x => x.SendAsync(It.IsAny<CsvDataFileSplitJob>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Logs_exception_and_marks_failed_when_split_message_fails()
    {
        var job = CreateJob(splitType: SplitType.ByLines, splitValue: 10);
        var ex = new InvalidOperationException("Split message failed.");

        _splitProducer.Setup(x => x.SendAsync(It.IsAny<CsvDataFileSplitJob>(), It.IsAny<CancellationToken>()))
                      .ThrowsAsync(ex);

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _progress.AsyncVerify(x => x.MarkSucceeded(job.JobId, job.SourceKey), Times.Once);
        await _progress.AsyncVerify(x => x.MarkFailed(job.JobId, job.SourceKey, ex.Message), Times.Once);

        await _logger.AsyncVerify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains($"Failed to import {job.SourceKey}")),
                ex,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task Processes_multiple_jobs()
    {
        var job1 = CreateJob(1);
        var job2 = CreateJob(2);

        await _sut.StartAsync(CancellationToken.None);

        await Write(job1);
        await Write(job2);

        await _copy.AsyncVerify(x => x.ExecAsync(It.IsAny<CsvDataFileImportJob>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        await _copy.AsyncVerify(x => x.ExecAsync(job1, It.IsAny<CancellationToken>()), Times.Once);
        await _copy.AsyncVerify(x => x.ExecAsync(job2, It.IsAny<CancellationToken>()), Times.Once);

        await _progress.AsyncVerify(x => x.MarkSucceeded(It.IsAny<string>(), It.IsAny<string>()), Times.Exactly(2));
    }

    [Fact]
    public async Task Uses_target_key_as_folder_when_no_extension()
    {
        var job = CreateJob(
            targetKey: "imported/example-file",
            splitType: SplitType.BySize,
            splitValue: 25);

        var expected = new CsvDataFileSplitJob(
            JobId: job.JobId,
            Key: job.TargetKey,
            TargetFolder: "example-file",
            SplitType: SplitType.BySize,
            SplitValue: 25);

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _splitProducer.AsyncVerify(x => x.SendAsync(expected, It.IsAny<CancellationToken>()), Times.Once);
    }

    private Task Write(CsvDataFileImportJob job) =>
        _channel.Writer.WriteAsync(job).AsTask();

    private static CsvDataFileImportJob CreateJob(
        int jobNo = 1,
        string? sourceKey = null,
        string? targetKey = null,
        SplitType splitType = SplitType.None,
        int? splitValue = null)
        => new(
            JobId: $"job-{jobNo}",
            SourceKey: sourceKey ?? $"incoming/file-{jobNo}.csv",
            TargetKey: targetKey ?? $"imported/file-{jobNo}.csv",
            Password: "test-password",
            Salt: "test-salt",
            SplitType: splitType,
            SplitValue: splitValue);

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.Complete();
        await _sut.StopAsync(CancellationToken.None);
        _sut.Dispose();
        GC.SuppressFinalize(this);
    }
}