using System.Threading.Channels;
using CadsBridge.Application.DataSeed.Services;
using CadsBridge.Application.Models;
using CadsBridge.Infrastructure.DataSeed.Services;
using CadsBridge.Testing.Support.Utilities.Assertions;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.DataSeed.Services;

public class DataSeedImportBackgroundServiceTests : IAsyncDisposable
{
    private readonly Channel<DataSeedImportJob> _channel;
    private readonly Mock<IFileSystemToS3CopyService> _copyService;
    private readonly Mock<ILogger<DataSeedImportBackgroundService>> _logger;
    private readonly DataSeedImportBackgroundService _sut;
    private DataSeedImportJob _job1 = CreateJob();

    public DataSeedImportBackgroundServiceTests()
    {
        _channel = Channel.CreateUnbounded<DataSeedImportJob>();
        _copyService = new Mock<IFileSystemToS3CopyService>();
        _logger = new Mock<ILogger<DataSeedImportBackgroundService>>();
        _logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        _copyService
            .Setup(x => x.ExecuteAsync(
                It.IsAny<DataSeedImportJob>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        _sut = new DataSeedImportBackgroundService(
            _channel,
            _copyService.Object,
            _logger.Object);
    }

    [Fact]
    public async Task DataSeedImportService_WhenJobIsReceived_ExecutesDataSeedFileCopyService()
    {
        // Arrange
        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(_job1, TestContext.Current.CancellationToken);

        // Assert
        await _copyService.AsyncVerify(x => x.ExecuteAsync(_job1, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task DataSeedImportService_WhenCopyServiceReturnsFalse_LogsFailure()
    {
        // Arrange
        _copyService
            .Setup(x => x.ExecuteAsync(_job1, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(_job1, TestContext.Current.CancellationToken);

        // Assert
        await _copyService.AsyncVerify(x => x.ExecuteAsync(_job1, It.IsAny<CancellationToken>()), Times.Once);
        await _logger.AsyncVerify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((value, _) =>
                    value.ToString()!.Contains("Failed")),
                null,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task DataSeedImportService_WhenCopyServiceThrows_LogsException()
    {
        // Arrange
        var exception = new InvalidOperationException("Copy failed.");

        _copyService
            .Setup(x => x.ExecuteAsync(_job1, It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);

        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(_job1, TestContext.Current.CancellationToken);

        // Assert
        await _copyService.AsyncVerify(x => x.ExecuteAsync(_job1, It.IsAny<CancellationToken>()), Times.Once);

        await _logger.AsyncVerify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((value, _) =>
                    value.ToString()!.Contains("Failed")),
                exception,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task DataSeedImportService_WhenMultipleJobsAreReceived_ExecutesDataSeedFileCopyServiceForEachJob()
    {
        // Arrange
        var firstJob = CreateJob(1);
        var secondJob = CreateJob(2);

        await _sut.StartAsync(TestContext.Current.CancellationToken);

        // Act
        await _channel.Writer.WriteAsync(firstJob, TestContext.Current.CancellationToken);
        await _channel.Writer.WriteAsync(secondJob, TestContext.Current.CancellationToken);

        // Assert
        await _copyService.AsyncVerify(x => x.ExecuteAsync(It.IsAny<DataSeedImportJob>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        await _copyService.AsyncVerify(x => x.ExecuteAsync(firstJob, It.IsAny<CancellationToken>()), Times.Once);
        await _copyService.AsyncVerify(x => x.ExecuteAsync(secondJob, It.IsAny<CancellationToken>()), Times.Once);
    }

    private static DataSeedImportJob CreateJob(int jobNo = 1)
    {
        return new DataSeedImportJob(
            JobId: $"job-{jobNo}",
            FileName: $"00{jobNo}_seed.sql",
            TargetKey: $"data-seed/00{jobNo}_seed.sql");
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.Complete();
        await _sut.StopAsync(TestContext.Current.CancellationToken);
        _sut?.Dispose();
        GC.SuppressFinalize(this);
    }
}