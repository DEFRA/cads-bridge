using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Worker.Tasks;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Worker.Tests.Unit.Tasks;

public class BulkScanTaskTests
{
    private readonly Mock<IFileDiscoveryService> _fileDiscoveryServiceMock = new();
    private readonly Mock<ILogger<BulkScanTask>> _loggerMock = new();

    private BulkScanTask CreateSut() =>
        new(_fileDiscoveryServiceMock.Object, _loggerMock.Object);

    [Fact]
    public async Task RunAsync_CallsFileDiscovery_WithCancellationToken()
    {
        // Arrange
        _fileDiscoveryServiceMock
            .Setup(x => x.GetFileNames(TestContext.Current.CancellationToken))
            .ReturnsAsync([]);

        var sut = CreateSut();

        // Act
        await sut.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        _fileDiscoveryServiceMock.Verify(
            x => x.GetFileNames(TestContext.Current.CancellationToken),
            Times.Once);
    }
}