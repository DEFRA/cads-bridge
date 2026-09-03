using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Testing.Support.Utilities.Logging;
using CadsBridge.Worker.Tasks;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Worker.Tests.Unit.Tasks;

public class BulkScanTaskTests
{
    private readonly Mock<IFileDiscoveryService> _fileDiscoveryServiceMock = new();
    private readonly Mock<ILogger<BulkFileScanTask>> _loggerMock = new Mock<ILogger<BulkFileScanTask>>().EnableAllLogLevels();

    private const string Prefix = "cads/cts/bulk";
    private const string DestinationPrefix = "import/cts/bulk";
    private const string ValidFileName = "CTSM_UKV_PROD_BULK_######_CT_REGISTERED_ANIMALS_2026-02-22-074603.csv";
    private string ValidObjectKey = $"{Prefix}/{ValidFileName}";

    private BulkFileScanTask CreateSut(List<string> fileNames)
    {
        _fileDiscoveryServiceMock
            .Setup(x => x.GetFileNames(Prefix, TestContext.Current.CancellationToken))
            .ReturnsAsync(fileNames);
        // Register the catch-all first: Moq gives precedence to the most-recently configured matching setup
        _fileDiscoveryServiceMock
            .Setup(x => x.IsFileValid(It.IsAny<string>(), TestContext.Current.CancellationToken))
            .ReturnsAsync(false);
        _fileDiscoveryServiceMock
            .Setup(x => x.IsFileValid(ValidFileName, TestContext.Current.CancellationToken))
            .ReturnsAsync(true);
        return new BulkFileScanTask(_fileDiscoveryServiceMock.Object, _loggerMock.Object);
    }

    [Fact]
    public async Task RunAsync_CallsFileDiscovery_WithCancellationToken()
    {
        // Arrange
        _fileDiscoveryServiceMock
            .Setup(x => x.GetFileNames(Prefix, TestContext.Current.CancellationToken))
            .ReturnsAsync([]);

        var sut = CreateSut(new List<string>());

        // Act
        await sut.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        _fileDiscoveryServiceMock.Verify(
            x => x.GetFileNames(Prefix, TestContext.Current.CancellationToken),
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
            x => x.GetFileNames(Prefix, TestContext.Current.CancellationToken),
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
            x => x.GetFileNames(Prefix, TestContext.Current.CancellationToken),
            Times.Once);

        _fileDiscoveryServiceMock.Verify(
            x => x.IsFileValid(It.IsAny<string>(), TestContext.Current.CancellationToken),
            Times.Once);
    }

    [Fact]
    public async Task RunAsync_EnqueuesValidFile_WhenFileIsValid()
    {
        // Arrange
        var objectKeys = new List<string> { ValidObjectKey };
        var sut = CreateSut(objectKeys);

        // Act
        await sut.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        _fileDiscoveryServiceMock.Verify(
            x => x.EnQueueFileImportMessages(
                It.Is<IReadOnlyList<string>>(list => list.Count == 1 && list[0] == ValidObjectKey),
                DestinationPrefix,
                TestContext.Current.CancellationToken),
            Times.Once);
    }

    [Fact]
    public async Task RunAsync_DoesNotEnqueue_WhenAllValidFilesAreFilteredOutByIsFileValid()
    {
        // Arrange - a valid CTSM bulk filename, but the discovery service reports it as already processed
        const string alreadyProcessedFile = "CTSM_UKV_PROD_BULK_ABC_0001_CT_OTHER_2026-02-22-074603.csv";
        var fileNames = new List<string> { alreadyProcessedFile };
        var sut = CreateSut(fileNames);

        // Act
        await sut.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        _fileDiscoveryServiceMock.Verify(
            x => x.IsFileValid(alreadyProcessedFile, TestContext.Current.CancellationToken),
            Times.Once);

        _fileDiscoveryServiceMock.Verify(
            x => x.EnQueueFileImportMessages(It.IsAny<IReadOnlyList<string>>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task RunAsync_EnqueuesOnlyValidFiles_WhenMixOfValidAndInvalidFilesReturned()
    {
        // Arrange
        const string ignoredFile = "CTSM_UKV_PROD_BULK_ABC_0001_CT_OTHER_2026-02-22-074603.csv";
        var fileNames = new List<string> { ValidFileName, ignoredFile };
        var sut = CreateSut(fileNames);

        // Act
        await sut.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        _fileDiscoveryServiceMock.Verify(
            x => x.EnQueueFileImportMessages(
                It.Is<IReadOnlyList<string>>(list => list.Count == 1 && list[0] == ValidFileName),
                DestinationPrefix,
                TestContext.Current.CancellationToken),
            Times.Once);
    }
}