using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Testing.Support.Utilities.Assertions;
using CadsBridge.Testing.Support.Utilities.Aws;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services;

public class CsvDataFileSplitterServiceTests
{
    private const string BucketName = "internal-bucket";
    private const string SourceKey = "imports/source-file.csv";
    private const string DestinationPrefix = "split-output";

    private static string SmallInputFile => string.Join(
        Environment.NewLine,
        "HEADER|ignored",
        "C|RECORD_TYPE|COLUMN_ONE|COLUMN_TWO",
        "D|1|One") + Environment.NewLine;

    [Fact]
    public async Task SplitFile_WhenSplitValueIsNull_ThrowsException()
    {
        // Arrange
        var s3 = new Mock<IAmazonS3>();
        var sut = GetSut(s3, new Mock<IS3ClientFactory>());
        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, null);

        // Act
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await sut.ExecuteAsync(request, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SplitFile_WhenSplitTypeIsInvalid_ThrowsOnFailure()
    {
        // Arrange
        const int linesPerChunk = 2;
        var (s3, factory, uploadedObjects) = S3MockBuilder.Create();
        var sut = GetSut(s3, factory);
        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, (SplitType)99, linesPerChunk);

        // Act
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await sut.ExecuteAsync(request, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SplitFile_WhenFileDoesNotExist_RetriesUntilFailure()
    {
        // Arrange
        const int linesPerChunk = 2;
        var (s3, factory, uploadedObjects) = S3MockBuilder.Create();
        var sut = GetSut(s3, factory);
        s3.Setup(x => x.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>())).Throws<NullReferenceException>();
        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, linesPerChunk);

        // Act
        await Assert.ThrowsAsync<NullReferenceException>(async () =>
            await sut.ExecuteAsync(request, TestContext.Current.CancellationToken));

        // Assert
        s3.Verify(client => client.GetObjectAsync(
                It.Is<GetObjectRequest>(x => x.BucketName == BucketName && x.Key == SourceKey),
                It.IsAny<CancellationToken>()),
            Times.Exactly(3));
    }

    [Fact]
    public async Task SplitFileByLineAsync_SplitsFileIntoChunksWithProcessedColumnDefinitions()
    {
        // Arrange
        const int linesPerChunk = 2;
        var sourceContent = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "C|RECORD_TYPE|FIRST_NAME|LAST_NAME",
            "D|Alice|Smith",
            "D|Bob|Jones",
            "D|Charlie|Brown",
            "D|Dana|White") + Environment.NewLine;

        var (s3, factory, uploadedObjects) = S3MockBuilder.Create(sourceContent);
        var sut = GetSut(s3, factory);
        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, linesPerChunk);

        // Act
        var result = await sut.ExecuteAsync(request, TestContext.Current.CancellationToken);

        // Assert
        result.Should().BeTrue();
        uploadedObjects.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["split-output/source-file.part-0001.csv"] = string.Join(
                Environment.NewLine,
                "record_type|first_name|last_name",
                "D|Alice|Smith",
                "D|Bob|Jones",
                string.Empty),
            ["split-output/source-file.part-0002.csv"] = string.Join(
                Environment.NewLine,
                "record_type|first_name|last_name",
                "D|Charlie|Brown",
                "D|Dana|White",
                string.Empty)
        });
    }

    [Fact]
    public async Task DataSeedImportService_WhenJobIsCancelled_Aborts()
    {
        // Arrange
        using var cancellationTokenSource = new CancellationTokenSource();
        await cancellationTokenSource.CancelAsync();
        var (s3, factory, uploadedObjects) = S3MockBuilder.Create(SmallInputFile);
        var sut = GetSut(s3, factory);
        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.BySize, 1);

        // Act
        var result = await sut.ExecuteAsync(request, cancellationTokenSource.Token);

        // Assert
        result.Should().BeFalse();
        await s3.AsyncVerify(x => x.GetObjectAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        uploadedObjects.Should().BeEmpty();
    }

    [Theory]
    [InlineData("")]
    [InlineData("HEADER|ignored\n")]
    [InlineData("HEADER|ignored\n" + "C|RECORD_TYPE|FIRST_NAME|LAST_NAME\n")]
    public async Task SplitFileByLineAsync_WhenFileDoesNotHaveContent_DoesNotUploadChunks(string sourceContent)
    {
        // Arrange
        const int linesPerChunk = 2;
        var (s3, factory, uploadedObjects) = S3MockBuilder.Create(sourceContent);
        var sut = GetSut(s3, factory);
        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, linesPerChunk);

        // Act
        await sut.ExecuteAsync(request, TestContext.Current.CancellationToken);

        // Assert
        uploadedObjects.Should().BeEmpty();
    }

    [Fact]
    public async Task SplitFileByLineAsync_WhenFinalChunkIsPartial_UploadsRemainingLines()
    {
        // Arrange
        const string sourceKey = "source-file.csv";
        const int linesPerChunk = 2;

        var sourceContent = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "C|RECORD_TYPE|COLUMN_ONE|COLUMN_TWO",
            "D|1|One",
            "D|2|Two",
            "D|3|Three") + Environment.NewLine;

        var (s3, factory, uploadedObjects) = S3MockBuilder.Create(sourceContent);
        var sut = GetSut(s3, factory);
        var request = new CsvDataFileSplitJob("", sourceKey, DestinationPrefix, SplitType.ByLines, linesPerChunk);

        // Act
        await sut.ExecuteAsync(
            request,
            TestContext.Current.CancellationToken);

        // Assert
        uploadedObjects.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            [$"{DestinationPrefix}/source-file.part-0001.csv"] = string.Join(
                Environment.NewLine,
                "record_type|column_one|column_two",
                "D|1|One",
                "D|2|Two",
                string.Empty),
            [$"{DestinationPrefix}/source-file.part-0002.csv"] = string.Join(
                Environment.NewLine,
                "record_type|column_one|column_two",
                "D|3|Three",
                string.Empty)
        });
    }

    [Fact]
    public async Task SplitFileByLineAsync_WhenFileHasNoDataLines_DoesNotUploadChunks()
    {
        // Arrange
        var sourceContent = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "C|RECORD_TYPE|COLUMN_ONE|COLUMN_TWO") + Environment.NewLine;

        var (s3, factory, uploadedObjects) = S3MockBuilder.Create(sourceContent);
        var sut = GetSut(s3, factory);
        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, 2);

        // Act
        await sut.ExecuteAsync(request, TestContext.Current.CancellationToken);

        // Assert
        s3.Verify(client => client.PutObjectAsync(
                It.IsAny<PutObjectRequest>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task SplitFileBySizeAsync_WhenFileFitsInSingleChunk_UploadsOneChunk()
    {
        // Arrange
        const int chunkSizeMb = 1;
        var (s3, factory, uploadedObjects) = S3MockBuilder.Create(SmallInputFile);
        var sut = GetSut(s3, factory);
        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.BySize, chunkSizeMb);

        // Act
        await sut.ExecuteAsync(
            request,
            TestContext.Current.CancellationToken);

        // Assert
        uploadedObjects.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            [$"{DestinationPrefix}/source-file.part-0001.csv"] = SmallInputFile
        });
    }

    [Fact]
    public async Task SplitFileBySizeAsync_WhenDestinationPrefixIsEmpty_UploadsChunkWithoutPrefix()
    {
        // Arrange
        const int chunkSizeMb = 1;
        var (s3, factory, uploadedObjects) = S3MockBuilder.Create(SmallInputFile);
        var sut = GetSut(s3, factory);
        var request = new CsvDataFileSplitJob("", SourceKey, "", SplitType.BySize, chunkSizeMb);

        // Act
        await sut.ExecuteAsync(request, TestContext.Current.CancellationToken);

        // Assert
        uploadedObjects.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["source-file.part-0001.csv"] = SmallInputFile
        });
    }

    [Fact]
    public async Task SplitFileBySizeAsync_WhenFileExceedsChunkSize_UploadsMultipleChunks()
    {
        // Arrange
        const string sourceKey = "imports/source-file.csv";
        const int chunkSizeMb = 1;
        var firstLine = new string('A', 600_000);
        var secondLine = new string('B', 600_000);
        var thirdLine = new string('C', 100);
        var sourceContent = string.Join(Environment.NewLine, firstLine, secondLine, thirdLine, Environment.NewLine);

        var (s3, factory, uploadedObjects) = S3MockBuilder.Create(sourceContent);
        var sut = GetSut(s3, factory);
        var request = new CsvDataFileSplitJob("", sourceKey, DestinationPrefix, SplitType.BySize, chunkSizeMb);

        // Act
        await sut.ExecuteAsync(request, TestContext.Current.CancellationToken);

        // Assert
        uploadedObjects.Keys.Should().BeEquivalentTo([
            $"{DestinationPrefix}/source-file.part-0001.csv",
            $"{DestinationPrefix}/source-file.part-0002.csv"
        ]);

        uploadedObjects[$"{DestinationPrefix}/source-file.part-0001.csv"].Should().Contain(firstLine)
            .And.NotContain(secondLine);

        uploadedObjects[$"{DestinationPrefix}/source-file.part-0002.csv"].Should().Contain(secondLine)
            .And.Contain(thirdLine);
    }

    private static CsvDataFileSplitterService GetSut(Mock<IAmazonS3> s3, Mock<IS3ClientFactory> s3ClientFactory)
    {
        var logger = new Mock<ILogger<CsvDataFileSplitterService>>();
        logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);

        return new CsvDataFileSplitterService(
            s3ClientFactory.Object,
            logger.Object);
    }

}