using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.Services;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Application.Tests.Unit;

public class FileSplitterTests
{
    [Fact]
    public async Task SplitFileByLineAsync_SplitsFileIntoChunksWithProcessedColumnDefinitions()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string sourceKey = "imports/source-file.csv";
        const string destinationPrefix = "split-output";
        const int linesPerChunk = 2;

        var sourceContent = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "RECORD_TYPE|FIRST_NAME|LAST_NAME",
            "D|Alice|Smith",
            "D|Bob|Jones",
            "D|Charlie|Brown",
            "D|Dana|White") + Environment.NewLine;

        var (s3, uploadedObjects) = CreateS3Mock(sourceContent);

        var sut = new FileSplitter(Mock.Of<ILogger<FileSplitter>>());

        // Act
        await sut.SplitFileByLineAsync(
            s3.Object,
            bucketName,
            sourceKey,
            destinationPrefix,
            linesPerChunk);

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

        s3.Verify(client => client.GetObjectAsync(
                It.Is<GetObjectRequest>(request =>
                    request.BucketName == bucketName &&
                    request.Key == sourceKey),
                It.IsAny<CancellationToken>()),
            Times.Once);

        s3.Verify(client => client.PutObjectAsync(
                It.Is<PutObjectRequest>(request =>
                    request.BucketName == bucketName &&
                    request.ContentType == "text/csv"),
                It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    [Fact]
    public async Task SplitFileByLineAsync_WhenFinalChunkIsPartial_UploadsRemainingLines()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string sourceKey = "source-file.csv";
        const string destinationPrefix = "chunks";
        const int linesPerChunk = 2;

        var sourceContent = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "RECORD_TYPE|COLUMN_ONE|COLUMN_TWO",
            "D|1|One",
            "D|2|Two",
            "D|3|Three") + Environment.NewLine;

        var (s3, uploadedObjects) = CreateS3Mock(sourceContent);

        var sut = new FileSplitter(Mock.Of<ILogger<FileSplitter>>());

        // Act
        await sut.SplitFileByLineAsync(
            s3.Object,
            bucketName,
            sourceKey,
            destinationPrefix,
            linesPerChunk);

        // Assert
        uploadedObjects.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["chunks/source-file.part-0001.csv"] = string.Join(
                Environment.NewLine,
                "column_one|column_two",
                "D|1|One",
                "D|2|Two",
                string.Empty),
            ["chunks/source-file.part-0002.csv"] = string.Join(
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

        var (s3, _) = CreateS3Mock(sourceContent);

        var sut = new FileSplitter(Mock.Of<ILogger<FileSplitter>>());

        // Act
        await sut.SplitFileByLineAsync(
            s3.Object,
            "test-bucket",
            "source-file.csv",
            "chunks",
            linesPerChunk: 2);

        // Assert
        s3.Verify(client => client.PutObjectAsync(
                It.IsAny<PutObjectRequest>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    private static (Mock<IAmazonS3> S3, Dictionary<string, string> UploadedObjects) CreateS3Mock(
        string sourceContent)
    {
        var uploadedObjects = new Dictionary<string, string>();

        var s3 = new Mock<IAmazonS3>();

        s3.Setup(client => client.GetObjectAsync(
                It.IsAny<GetObjectRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new GetObjectResponse
            {
                ResponseStream = new MemoryStream(Encoding.UTF8.GetBytes(sourceContent))
            });

        s3.Setup(client => client.PutObjectAsync(
                It.IsAny<PutObjectRequest>(),
                It.IsAny<CancellationToken>()))
            .Callback<PutObjectRequest, CancellationToken>((request, _) =>
            {
                using var reader = new StreamReader(
                    request.InputStream,
                    Encoding.UTF8,
                    detectEncodingFromByteOrderMarks: true,
                    leaveOpen: true);

                uploadedObjects[request.Key] = reader.ReadToEnd();
            })
            .ReturnsAsync(new PutObjectResponse());

        return (s3, uploadedObjects);
    }
}