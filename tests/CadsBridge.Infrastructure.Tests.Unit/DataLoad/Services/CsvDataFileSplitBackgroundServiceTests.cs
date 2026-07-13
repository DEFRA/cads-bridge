using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Testing.Support.Utilities.Assertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Threading.Channels;
using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Infrastructure.DataLoad.Configuration;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services;

public class CsvDataFileSplitBackgroundServiceTests : IAsyncDisposable
{
    private readonly Channel<CsvDataFileSplitJob> _channel = Channel.CreateUnbounded<CsvDataFileSplitJob>();
    private readonly Mock<ILogger<CsvDataFileSplitBackgroundService>> _logger = new();
    private readonly Mock<ISplitJobProgressStore> _progress = new();
    private readonly Mock<IFileImportStatusStore> _fileImportStatusStore = new();
    private readonly Mock<ICsvDataFileSplitterService> _splitter = new();
    private readonly CsvDataFileSplitBackgroundService _sut;

    public CsvDataFileSplitBackgroundServiceTests()
    {
        _logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);

        _splitter.Setup(x => x.ExecuteAsync(It.IsAny<CsvDataFileSplitJob>(), It.IsAny<CancellationToken>()))
                 .ReturnsAsync(true);

        _fileImportStatusStore.Setup(x => x.MarkSucceeded(It.IsAny<long>(), It.IsAny<CancellationToken>()))
                              .Returns(Task.CompletedTask);
        _fileImportStatusStore.Setup(x => x.MarkFailed(It.IsAny<long>(), It.IsAny<CancellationToken>()))
                              .Returns(Task.CompletedTask);

        var config = new DataLoadConfiguration { MaxParallelDownloads = 4 };

        _sut = new CsvDataFileSplitBackgroundService(
            _channel,
            _logger.Object,
            _progress.Object,
            _fileImportStatusStore.Object,
            _splitter.Object,
            config);
    }

    [Fact]
    public async Task Marks_failed_when_filestatusId_is_null()
    {
        await _sut.StartAsync(CancellationToken.None);

        var job = CreateJob(1, fileImportStatusId: null);
        await Write(job);

        await _progress.AsyncVerify(x => x.MarkFailed(job.JobId, job.SourceKey, It.IsAny<string>()), Times.Once);

        _splitter.Verify(x => x.ExecuteAsync(It.IsAny<CsvDataFileSplitJob>(), It.IsAny<CancellationToken>()), Times.Never);
        _fileImportStatusStore.Verify(x => x.MarkFailed(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
        _fileImportStatusStore.Verify(x => x.MarkSucceeded(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Processes_job_successfully()
    {
        await _sut.StartAsync(CancellationToken.None);

        var job = CreateJob(1);
        await Write(job);

        await _progress.AsyncVerify(x => x.MarkInProgress(job.JobId, job.SourceKey), Times.Once);
        await _splitter.AsyncVerify(x => x.ExecuteAsync(job, It.IsAny<CancellationToken>()), Times.Once);
        await _progress.AsyncVerify(x => x.MarkSucceeded(job.JobId, job.SourceKey), Times.Once);
        await _fileImportStatusStore.AsyncVerify(x => x.MarkSucceeded(job.FileImportStatusId!.Value, It.IsAny<CancellationToken>()), Times.Once);

        _progress.Verify(x => x.MarkFailed(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()), Times.Never);
        _fileImportStatusStore.Verify(x => x.MarkFailed(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
    }


    [Fact]
    public async Task Marks_failed_when_splitter_returns_false()
    {
        var job = CreateJob(1);

        _splitter.Setup(x => x.ExecuteAsync(job, It.IsAny<CancellationToken>()))
                 .ReturnsAsync(false);

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _progress.AsyncVerify(x => x.MarkInProgress(job.JobId, job.SourceKey), Times.Once);
        await _progress.AsyncVerify(x => x.MarkFailed(job.JobId, job.SourceKey, It.IsAny<string>()), Times.Once);
        await _fileImportStatusStore.AsyncVerify(x => x.MarkFailed(job.FileImportStatusId!.Value, It.IsAny<CancellationToken>()), Times.Once);

        _progress.Verify(x => x.MarkSucceeded(It.IsAny<string>(), It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task Logs_exception_and_marks_failed_when_splitter_throws()
    {
        var job = CreateJob(1);
        var ex = new InvalidOperationException("Split failed");

        _splitter.Setup(x => x.ExecuteAsync(job, It.IsAny<CancellationToken>()))
                 .ThrowsAsync(ex);

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _splitter.AsyncVerify(x => x.ExecuteAsync(job, It.IsAny<CancellationToken>()), Times.Once);
        await _progress.AsyncVerify(x => x.MarkFailed(job.JobId, job.SourceKey, It.IsAny<string>()), Times.Once);
        await _fileImportStatusStore.AsyncVerify(x => x.MarkFailed(job.FileImportStatusId!.Value, It.IsAny<CancellationToken>()), Times.Once);

        await _logger.AsyncVerify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains($"Failed to split file {job.SourceKey}")),
                ex,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task Processes_multiple_jobs_independently()
    {
        var job1 = CreateJob(1);
        var job2 = CreateJob(2);

        await _sut.StartAsync(CancellationToken.None);

        await Write(job1);
        await Write(job2);

        await _splitter.AsyncVerify(x => x.ExecuteAsync(It.IsAny<CsvDataFileSplitJob>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        await _splitter.AsyncVerify(x => x.ExecuteAsync(job1, It.IsAny<CancellationToken>()), Times.Once);
        await _splitter.AsyncVerify(x => x.ExecuteAsync(job2, It.IsAny<CancellationToken>()), Times.Once);

        await _progress.AsyncVerify(x => x.MarkSucceeded(It.IsAny<string>(), It.IsAny<string>()), Times.Exactly(2));
    }

    private Task Write(CsvDataFileSplitJob job) =>
        _channel.Writer.WriteAsync(job).AsTask();

    private static CsvDataFileSplitJob CreateJob(int n, long? fileImportStatusId = 1) =>
        new(
            JobId: $"job-{n}",
            SourceKey: $"import/file-{n}.csv",
            FileImportStatusId: fileImportStatusId);

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.Complete();
        await _sut.StopAsync(CancellationToken.None);
        _sut.Dispose();
        GC.SuppressFinalize(this);
    }
}