using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.Models;
using CadsBridge.Application.Services;
using CadsBridge.Core.Storage.Abstractions;
using CadsBridge.Core.Storage.Clients;
using CadsBridge.Core.Storage.Factories;
using CadsBridge.Testing.Support.Utilities.Aws;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Application.Tests.Unit.Services;

public class S3FileSplitterServiceTests
{
    const string bucketName = "test-bucket";
    private const string SourceKey = "imports/source-file.csv";
    private const string DestinationPrefix = "split-output";

    [Fact]
    public async Task SplitFile_WhenSplitValueIsNull_ThrowsException()
    {
        // Arrange
        var s3 = new Mock<IAmazonS3>();
        var sut = GetSut(s3);
        var request = new FileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, null);

        // Act
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await sut.ExecuteAsync(request, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SplitFile_WhenSplitTypeIsInvalid_ThrowsOnFailure()
    {
        // Arrange
        const int linesPerChunk = 2;
        var s3 = new Mock<IAmazonS3>();
        var sut = GetSut(s3);
        var request = new FileSplitJob("", SourceKey, DestinationPrefix, (SplitType)99, linesPerChunk);

        // Act
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await sut.ExecuteAsync(request, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SplitFile_WhenFileDoesNotExist_RetriesUntilFailure()
    {
        // Arrange
        const int linesPerChunk = 2;
        var s3 = new Mock<IAmazonS3>();
        var sut = GetSut(s3);
        var request = new FileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, linesPerChunk);

        // Act
        await Assert.ThrowsAsync<NullReferenceException>(async () =>
            await sut.ExecuteAsync(request, TestContext.Current.CancellationToken));

        // Assert
        s3.Verify(client => client.GetObjectAsync(
                It.Is<GetObjectRequest>(x => x.BucketName == bucketName && x.Key == SourceKey),
                It.IsAny<CancellationToken>()),
            Times.Exactly(3));
    }

    // TODO - should first column be removed?????
    [Fact]
    public async Task SplitFileByLineAsync_SplitsFileIntoChunksWithProcessedColumnDefinitions()
    {
        // Arrange
        const int linesPerChunk = 2;
        var sourceContent = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "RECORD_TYPE|FIRST_NAME|LAST_NAME",
            "D|Alice|Smith",
            "D|Bob|Jones",
            "D|Charlie|Brown",
            "D|Dana|White") + Environment.NewLine;

        var (s3, uploadedObjects) = S3MockBuilder.Create(sourceContent);
        var sut = GetSut(s3);
        var request = new FileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, linesPerChunk);

        // Act
        await sut.ExecuteAsync(request, TestContext.Current.CancellationToken);

        // Assert
        uploadedObjects.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["split-output/source-file.part-0001.csv"] = string.Join(
                Environment.NewLine,
                "first_name|last_name",
                "D|Alice|Smith",
                "D|Bob|Jones",
                string.Empty),
            ["split-output/source-file.part-0002.csv"] = string.Join(
                Environment.NewLine,
                "first_name|last_name",
                "D|Charlie|Brown",
                "D|Dana|White",
                string.Empty)
        });
    }

    [Theory]
    [InlineData("")]
    [InlineData("HEADER|ignored\n")]
    [InlineData("HEADER|ignored\n" + "RECORD_TYPE|FIRST_NAME|LAST_NAME\n")]
    public async Task SplitFileByLineAsync_WhenFileDoesNotHaveContent_DoesNotUploadChunks(string sourceContent)
    {
        // Arrange
        const int linesPerChunk = 2;
        var (s3, uploadedObjects) = ((Mock<IAmazonS3> s3, Dictionary<string, string> uploadedObjects))S3MockBuilder.Create(sourceContent);
        var sut = GetSut(s3);
        var request = new FileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, linesPerChunk);

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
            "RECORD_TYPE|COLUMN_ONE|COLUMN_TWO",
            "D|1|One",
            "D|2|Two",
            "D|3|Three") + Environment.NewLine;

        var (s3, uploadedObjects) = ((Mock<IAmazonS3> s3, Dictionary<string, string> uploadedObjects))S3MockBuilder.Create(sourceContent);
        var sut = GetSut(s3);
        var request = new FileSplitJob("", sourceKey, DestinationPrefix, SplitType.ByLines, linesPerChunk);

        // Act
        await sut.ExecuteAsync(
            request,
            TestContext.Current.CancellationToken);

        // Assert
        uploadedObjects.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            [$"{DestinationPrefix}/source-file.part-0001.csv"] = string.Join(
                Environment.NewLine,
                "column_one|column_two",
                "D|1|One",
                "D|2|Two",
                string.Empty),
            [$"{DestinationPrefix}/source-file.part-0002.csv"] = string.Join(
                Environment.NewLine,
                "column_one|column_two",
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
            "RECORD_TYPE|COLUMN_ONE|COLUMN_TWO") + Environment.NewLine;
        var (s3, _) = ((Mock<IAmazonS3> s3, Dictionary<string, string> uploadedObjects))S3MockBuilder.Create(sourceContent);
        var sut = GetSut(s3);
        var request = new FileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, 2);

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
        var sourceContent = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "RECORD_TYPE|COLUMN_ONE|COLUMN_TWO",
            "D|1|One") + Environment.NewLine;
        var (s3, uploadedObjects) = ((Mock<IAmazonS3> s3, Dictionary<string, string> uploadedObjects))S3MockBuilder.Create(sourceContent);
        var sut = GetSut(s3);
        var request = new FileSplitJob("", SourceKey, DestinationPrefix, SplitType.BySize, chunkSizeMb);

        // Act
        await sut.ExecuteAsync(
            request,
            TestContext.Current.CancellationToken);

        // Assert
        uploadedObjects.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            [$"{DestinationPrefix}/source-file.part-0001.csv"] = sourceContent
        });
    }

    [Fact]
    public async Task SplitFileBySizeAsync_WhenDestinationPrefixIsEmpty_UploadsChunkWithoutPrefix()
    {
        // Arrange
        const int chunkSizeMb = 1;
        var sourceContent = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "RECORD_TYPE|COLUMN_ONE|COLUMN_TWO",
            "D|1|One") + Environment.NewLine;

        var (s3, uploadedObjects) = ((Mock<IAmazonS3> s3, Dictionary<string, string> uploadedObjects))S3MockBuilder.Create(sourceContent);
        var sut = GetSut(s3);
        var request = new FileSplitJob("", SourceKey, "", SplitType.BySize, chunkSizeMb);

        // Act
        await sut.ExecuteAsync(request, TestContext.Current.CancellationToken);

        // Assert
        uploadedObjects.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["source-file.part-0001.csv"] = sourceContent
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

        var (s3, uploadedObjects) = ((Mock<IAmazonS3> s3, Dictionary<string, string> uploadedObjects))S3MockBuilder.Create(sourceContent);

        var sut = GetSut(s3);
        var request = new FileSplitJob("", sourceKey, DestinationPrefix, SplitType.BySize, chunkSizeMb);

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

    private static S3FileSplitterService GetSut(Mock<IAmazonS3> s3)
    {
        var s3ClientFactory = new Mock<IS3ClientFactory>();

        s3ClientFactory
            .Setup(x => x.GetClientInfo<InternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(s3.Object, bucketName));

        return new S3FileSplitterService(s3ClientFactory.Object,
            Mock.Of<ILogger<S3FileSplitterService>>());
    }
}