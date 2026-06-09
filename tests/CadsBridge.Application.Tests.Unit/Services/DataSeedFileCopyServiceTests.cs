using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.Models;
using CadsBridge.Application.Services;
using CadsBridge.Core.Storage.Abstractions;
using CadsBridge.Core.Storage.Clients;
using CadsBridge.Core.Storage.Factories;
using CadsBridge.Core.Storage.FileSystem;
using CadsBridge.Testing.Support.Utilities.Aws;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Application.Tests.Unit.Services;

public class DataSeedFileCopyServiceTests
{
    private const string BucketName = "test-bucket";

    private Mock<IFileSytemWrapper> _mockFileSystemWrapper = new();

    public DataSeedFileCopyServiceTests()
    {
        _mockFileSystemWrapper.Setup(x => x.OpenRead(It.IsAny<string>())).Throws<FileNotFoundException>();
    }

    [Fact]
    public async Task ExecuteAsync_WhenSourceFileExists_UploadsFileToConfiguredTargetKey()
    {
        // Arrange
        const string targetKey = "data-seed/001_seed.sql";
        const string fileContent = "FILE CONTENT;";
        var sourceFilePath = "abc/001_seed.sql";

        MockLocalFile(fileContent, sourceFilePath);
        var (s3, uploadedObjects) = S3MockBuilder.CreateCapturingUploads();

        var sut = GetSut(s3);

        var request = new DataSeedImportJob(
            JobId: "job-1",
            FileName: sourceFilePath,
            TargetKey: targetKey);

        // Act
        var result = await sut.ExecuteAsync(
            request,
            TestContext.Current.CancellationToken);

        // Assert
        result.Should().BeTrue();

        uploadedObjects.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            [targetKey] = fileContent
        });

        s3.Verify(client => client.PutObjectAsync(
                It.Is<PutObjectRequest>(putRequest =>
                    putRequest.BucketName == BucketName &&
                    putRequest.Key == targetKey &&
                    putRequest.ContentType == "text/plain"),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_WhenSourceFileDoesNotExist_ReturnsFalseAndDoesNotUploadObject()
    {
        // Arrange
        var missingFilePath = "abc/missing.sql";
        var (s3, uploadedObjects) = S3MockBuilder.CreateCapturingUploads();
        var sut = GetSut(s3);

        var request = new DataSeedImportJob(
            JobId: "job-1",
            FileName: missingFilePath,
            TargetKey: "data-seed/missing.sql");

        // Act
        var result = await sut.ExecuteAsync(request, TestContext.Current.CancellationToken);

        // Assert
        result.Should().BeFalse();
        uploadedObjects.Should().BeEmpty();
        s3.Verify(client => client.PutObjectAsync(
                It.IsAny<PutObjectRequest>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    private DataSeedFileCopyService GetSut(Mock<IAmazonS3> s3)
    {
        var s3ClientFactory = new Mock<IS3ClientFactory>();
        s3ClientFactory
            .Setup(x => x.GetClientInfo<InternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(s3.Object, BucketName));

        return new DataSeedFileCopyService(
            s3ClientFactory.Object,
            _mockFileSystemWrapper.Object,
            Mock.Of<ILogger<DataSeedFileCopyService>>());
    }

    private void MockLocalFile(string content, string path)
    {
        _mockFileSystemWrapper
            .Setup(x => x.OpenRead(path))
            .Returns(() => new MemoryStream(Encoding.UTF8.GetBytes(content)));
    }
}