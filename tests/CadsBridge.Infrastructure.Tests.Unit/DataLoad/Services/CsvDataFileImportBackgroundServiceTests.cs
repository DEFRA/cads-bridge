using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Messaging;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Testing.Support.Utilities.Assertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services;

public class CsvDataFileImportBackgroundServiceTests : IAsyncDisposable
{
    private const long DefaultFileImportId = 1L;
    private const long DefaultRecordCount = 0L;

    private readonly Channel<CsvDataFileImportJob> _channel = Channel.CreateUnbounded<CsvDataFileImportJob>();
    private readonly Mock<ILogger<CsvDataFileImportBackgroundService>> _logger = new();
    private readonly Mock<IFileImportStore> _fileImportStore = new();
    private readonly Mock<ISplitMessageProducer> _splitProducer = new();
    private readonly Mock<IS3FileMetaDataService> _s3FileMetaDataService = new();
    private readonly Mock<IS3CopyService> _copy = new();
    private readonly CsvDataFileImportBackgroundService _sut;

    public CsvDataFileImportBackgroundServiceTests()
    {
        _logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);

        _copy.Setup(x => x.ExecAsync(It.IsAny<CsvDataFileImportJob>(), It.IsAny<CancellationToken>()))
             .ReturnsAsync(1);

        _splitProducer.Setup(x => x.SendAsync(It.IsAny<CsvDataFileSplitJob>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        _s3FileMetaDataService.Setup(x => x.GetRecordCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);

        _fileImportStore.Setup(x => x.CreateAsync(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(DefaultFileImportId);
        _fileImportStore.Setup(x => x.MarkTransferredAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _fileImportStore.Setup(x => x.MarkSplitAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
           .Returns(Task.CompletedTask);
        _fileImportStore.Setup(x => x.MarkCompletedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _fileImportStore.Setup(x => x.MarkFailedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var config = new DataLoadConfiguration { MaxParallelDownloads = 4 };

        var serviceCollection = new ServiceCollection();
        serviceCollection.AddTransient<IS3CopyService>(f => _copy.Object);
        serviceCollection.AddTransient<IFileImportStore>(f => _fileImportStore.Object);
        serviceCollection.AddTransient<ISplitMessageProducer>(f => _splitProducer.Object);
        serviceCollection.AddTransient<IS3FileMetaDataService>(f => _s3FileMetaDataService.Object);
        var serviceProvider = serviceCollection.BuildServiceProvider();
        var serviceScopeFactory = serviceProvider.GetRequiredService<IServiceScopeFactory>();
        _sut = new CsvDataFileImportBackgroundService(
            _channel,
            _logger.Object,
            serviceScopeFactory,
            config);
    }

    [Fact]
    public async Task Marks_in_progress_and_executes_copy()
    {
        await _sut.StartAsync(CancellationToken.None);

        var job = CreateJob();
        await Write(job);

        await _fileImportStore.AsyncVerify(x => x.CreateAsync(job.SourceKey, 0, It.IsAny<CancellationToken>()), Times.Once);
        await _copy.AsyncVerify(x => x.ExecAsync(job, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Marks_succeeded_when_copy_succeeds()
    {
        await _sut.StartAsync(CancellationToken.None);

        var job = CreateJob();
        await Write(job);

        _fileImportStore.Verify(x => x.MarkFailedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Theory]
    [InlineData("incoming/example-file.csv", "import/example-file.csv")]
    [InlineData("incoming/example-file", "import/example-file")]
    public async Task Sends_split_message_with_computed_target_key_after_successful_copy(string sourceKey, string expectedTargetKey)
    {
        var job = CreateJob(sourceKey: sourceKey);

        var expected = new CsvDataFileSplitJob(
            SourceKey: expectedTargetKey,
            FileImportId: DefaultFileImportId,
            TotalRowsToProcess: 1);

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _splitProducer.AsyncVerify(x => x.SendAsync(expected, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Marks_failed_when_copy_returns_false()
    {
        var job = CreateJob();

        _copy.Setup(x => x.ExecAsync(job, It.IsAny<CancellationToken>()))
             .ReturnsAsync(It.IsAny<long>());

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _fileImportStore.AsyncVerify(x => x.MarkFailedAsync(DefaultFileImportId, It.IsAny<CancellationToken>()), Times.Once);

        _splitProducer.Verify(x => x.SendAsync(It.IsAny<CsvDataFileSplitJob>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Logs_exception_and_marks_failed_when_copy_throws()
    {
        var job = CreateJob();
        var ex = new InvalidOperationException("Copy failed.");

        _copy.Setup(x => x.ExecAsync(job, It.IsAny<CancellationToken>()))
             .ThrowsAsync(ex);

        _s3FileMetaDataService.Setup(x => x.GetRecordCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _fileImportStore.AsyncVerify(x => x.MarkFailedAsync(DefaultFileImportId, It.IsAny<CancellationToken>()), Times.Once);

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
        var job = CreateJob();
        var ex = new InvalidOperationException("Split message failed.");

        _splitProducer.Setup(x => x.SendAsync(It.IsAny<CsvDataFileSplitJob>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(ex);

        await _sut.StartAsync(CancellationToken.None);
        await Write(job);

        await _fileImportStore.AsyncVerify(x => x.MarkFailedAsync(DefaultFileImportId, It.IsAny<CancellationToken>()), Times.Once);

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
    }

    private Task Write(CsvDataFileImportJob job) =>
        _channel.Writer.WriteAsync(job).AsTask();

    private static CsvDataFileImportJob CreateJob(int jobNo = 1, string? sourceKey = null)
        => new(
            SourceKey: sourceKey ?? $"incoming/file-{jobNo}.csv");

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.Complete();
        await _sut.StopAsync(CancellationToken.None);
        _sut.Dispose();
        GC.SuppressFinalize(this);
    }
}