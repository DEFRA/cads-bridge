using CadsBridge.Core.Locking;
using CadsBridge.Testing.Support.Utilities.Logging;
using CadsBridge.Worker.Jobs;
using CadsBridge.Worker.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Quartz;

namespace CadsBridge.Tests.Component.Jobs;

public class BulkScanJobComponentTests
{
    private readonly Mock<IBulkFileScanTask> _bulkScanTaskMock = new();
    private readonly Mock<IDistributedLock> _distributedLockMock = new();
    private readonly Mock<ILogger<BulkScanJob>> _loggerMock = new Mock<ILogger<BulkScanJob>>().EnableAllLogLevels();
    private readonly Mock<IJobExecutionContext> _contextMock = new();

    public BulkScanJobComponentTests()
    {
        _contextMock.Setup(x => x.CancellationToken).Returns(CancellationToken.None);
    }

    private BulkScanJob CreateSut() =>
        new(_bulkScanTaskMock.Object, _distributedLockMock.Object, _loggerMock.Object);

    [Fact]
    public async Task Execute_RunsTask_WhenLockIsAcquired()
    {
        // Arrange
        _distributedLockMock
            .Setup(x => x.TryAcquireAsync(nameof(BulkScanJob), CancellationToken.None))
            .ReturnsAsync(true);

        _bulkScanTaskMock
            .Setup(x => x.RunAsync(CancellationToken.None))
            .Returns(Task.CompletedTask);

        var sut = CreateSut();

        // Act
        await sut.Execute(_contextMock.Object);

        // Assert
        _bulkScanTaskMock.Verify(x => x.RunAsync(CancellationToken.None), Times.Once);
    }

    [Fact]
    public async Task Execute_ReleasesLock_WhenTaskCompletes()
    {
        // Arrange
        _distributedLockMock
            .Setup(x => x.TryAcquireAsync(nameof(BulkScanJob), CancellationToken.None))
            .ReturnsAsync(true);

        var sut = CreateSut();

        // Act
        await sut.Execute(_contextMock.Object);

        // Assert
        _distributedLockMock.Verify(x => x.ReleaseAsync(nameof(BulkScanJob), CancellationToken.None), Times.Once);
    }

    [Fact]
    public async Task Execute_ReleasesLock_WhenTaskThrows()
    {
        // Arrange
        _distributedLockMock
            .Setup(x => x.TryAcquireAsync(nameof(BulkScanJob), CancellationToken.None))
            .ReturnsAsync(true);

        _bulkScanTaskMock
            .Setup(x => x.RunAsync(CancellationToken.None))
            .ThrowsAsync(new InvalidOperationException("task failed"));

        var sut = CreateSut();

        // Act
        var act = async () => await sut.Execute(_contextMock.Object);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>();
        _distributedLockMock.Verify(x => x.ReleaseAsync(nameof(BulkScanJob), CancellationToken.None), Times.Once);
    }

    [Fact]
    public async Task Execute_SkipsTask_WhenLockIsNotAcquired()
    {
        // Arrange
        _distributedLockMock
            .Setup(x => x.TryAcquireAsync(nameof(BulkScanJob), CancellationToken.None))
            .ReturnsAsync(false);

        var sut = CreateSut();

        // Act
        await sut.Execute(_contextMock.Object);

        // Assert
        _bulkScanTaskMock.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Execute_DoesNotReleaseLock_WhenLockWasNotAcquired()
    {
        // Arrange
        _distributedLockMock
            .Setup(x => x.TryAcquireAsync(nameof(BulkScanJob), CancellationToken.None))
            .ReturnsAsync(false);

        var sut = CreateSut();

        // Act
        await sut.Execute(_contextMock.Object);

        // Assert
        _distributedLockMock.Verify(x => x.ReleaseAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}