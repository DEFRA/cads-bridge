using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Testing.Support.Utilities.Logging;
using CadsBridge.Worker.Tasks;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Worker.Tests.Unit.Tasks;

public class BulkScanTaskTests
{
    private readonly Mock<IFileDiscoveryService> _fileDiscoveryServiceMock = new();
    private readonly Mock<ILogger<BulkScanTask>> _loggerMock = new Mock<ILogger<BulkScanTask>>().EnableAllLogLevels();

    private const string ValidFileName = "CTSM_UKV_PROD_BULK_######_CT_REGISTERED_ANIMALS_2026-02-22-074603.csv";

    private BulkScanTask CreateSut(List<string> fileNames)
    {
        _fileDiscoveryServiceMock
            .Setup(x => x.GetFileNames(TestContext.Current.CancellationToken))
            .ReturnsAsync(fileNames);
        _fileDiscoveryServiceMock
            .Setup(x => x.IsFileValid(ValidFileName, TestContext.Current.CancellationToken))
            .ReturnsAsync(true);
        _fileDiscoveryServiceMock
            .Setup(x => x.IsFileValid(It.IsAny<string>(), TestContext.Current.CancellationToken))
            .ReturnsAsync(false);
        return new BulkScanTask(_fileDiscoveryServiceMock.Object, _loggerMock.Object);
    }

    [Fact]
    public async Task RunAsync_CallsFileDiscovery_WithCancellationToken()
    {
        // Arrange
        _fileDiscoveryServiceMock
            .Setup(x => x.GetFileNames(TestContext.Current.CancellationToken))
            .ReturnsAsync([]);

        var sut = CreateSut(new List<string>());

        // Act
        await sut.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        _fileDiscoveryServiceMock.Verify(
            x => x.GetFileNames(TestContext.Current.CancellationToken),
            Times.Once);
    }

    [Fact]
    public async Task RunAsync_Gets_InvalidFileNames()
    {
        // Arrange
        var fileNames = new List<string> { "invalid1.csv", "invalid2.txt", "invalid2.csv" };

        var sut = CreateSut(fileNames);

        // Act
        await sut.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        _fileDiscoveryServiceMock.Verify(
            x => x.GetFileNames(TestContext.Current.CancellationToken),
            Times.Once);

        _fileDiscoveryServiceMock.Verify(
            x => x.IsFileValid(It.IsAny<string>(), TestContext.Current.CancellationToken),
            Times.Never);
    }

    [Fact]
    public async Task RunAsync_Gets_ValidFileName()
    {
        // Arrange
        var fileNames = new List<string> { ValidFileName };

        var sut = CreateSut(fileNames);

        // Act
        await sut.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        _fileDiscoveryServiceMock.Verify(
            x => x.GetFileNames(TestContext.Current.CancellationToken),
            Times.Once);

        _fileDiscoveryServiceMock.Verify(
            x => x.IsFileValid(It.IsAny<string>(), TestContext.Current.CancellationToken),
            Times.Once);
    }
}