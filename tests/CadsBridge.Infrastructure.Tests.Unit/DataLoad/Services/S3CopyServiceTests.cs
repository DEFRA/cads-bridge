using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.Storage.Transfer;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.Crypto;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Factories;
using CadsBridge.Infrastructure.Storage.Transfer;
using CadsBridge.Testing.Support.Utilities.Aws;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services;

public class S3CopyServiceTests
{
    private static readonly Mock<ITransferUtilityAdapter> s_transferUtilityAdapterMock = new();

    private const string ExternalBucketName = "external-bucket";
    private const string InternalBucketName = "internal-bucket";
    private const string SourceKey = "incoming/source-file.csv";
    private const string TargetKey = "imported/source-file.csv";
    private const string Password = "test-password";
    private const string Salt = "test-salt";

    [Fact]
    public async Task CopyWithRetryAsync_WhenFileIsSmallerThanSingleFileLimit_DecryptsAndUploadsWithPutObject()
    {
        // Arrange
        const string decryptedContent = "decrypted file content";
        var s3 = CreateS3Mock(encryptedContent: "encrypted file content", contentLength: 1024);
        var aesCryptoTransform = CreateAesCryptoTransformMock(decryptedContent);
        var sut = GetSut(s3, aesCryptoTransform);

        var request = CreateJob();

        // Act
        var result = await sut.ExecAsync(request, TestContext.Current.CancellationToken);

        // Assert
        result.Should().BeTrue();

        aesCryptoTransform.Verify(x => x.DecryptStreamAsync(
                It.IsAny<Stream>(),
                It.IsAny<Stream>(),
                Password,
                Salt,
                null,
                null,
                It.IsAny<CancellationToken>()),
            Times.Once);

        s3.Verify(client => client.GetObjectAsync(ExternalBucketName, SourceKey, It.IsAny<CancellationToken>()), Times.Once);
        s3.Verify(client => client.GetObjectMetadataAsync(ExternalBucketName, SourceKey, It.IsAny<CancellationToken>()), Times.Once);
        s3.Verify(client => client.PutObjectAsync(
                It.Is<PutObjectRequest>(request =>
                    request.BucketName == InternalBucketName &&
                    request.Key == TargetKey &&
                    request.ContentType == "text/plain"),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task CopyWithRetryAsync_WhenFileIsLarge_UsesMultipartUploadAndCalculatesPartSize()
    {
        // Arrange
        const long fileSize = 150L * 1024L * 1024L;
        var s3 = CreateS3Mock(encryptedContent: "large decrypted file content", contentLength: fileSize);
        var aesCryptoTransform = new Mock<IAesCryptoTransform>();
        var sut = GetSut(s3, aesCryptoTransform);
        var request = CreateJob();

        // Act
        var result = await sut.ExecAsync(request, TestContext.Current.CancellationToken);

        // Assert
        result.Should().BeTrue();

        s3.Verify(client => client.GetObjectMetadataAsync(
                ExternalBucketName,
                SourceKey,
                It.IsAny<CancellationToken>()),
            Times.Once);

        s_transferUtilityAdapterMock.Verify(x => x.UploadAsync(It.IsAny<TransferUtilityUploadRequest>(), It.IsAny<CancellationToken>()));

        s3.Verify(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CopyWithRetryAsync_WhenFirstAttemptFails_RetriesAndThenSucceeds()
    {
        // Arrange
        const string decryptedContent = "decrypted file content";

        var getObjectAttempts = 0;

        var s3 = CreateS3Mock(
            encryptedContent: "encrypted file content",
            contentLength: 1024);

        s3.Setup(client => client.GetObjectAsync(
                ExternalBucketName,
                SourceKey,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                getObjectAttempts++;

                if (getObjectAttempts == 1)
                {
                    throw new AmazonS3Exception("Temporary S3 failure");
                }

                return new GetObjectResponse
                {
                    ResponseStream = new MemoryStream(Encoding.UTF8.GetBytes("encrypted file content"))
                };
            });

        var aesCryptoTransform = CreateAesCryptoTransformMock(decryptedContent);

        var sut = GetSut(s3, aesCryptoTransform);

        // Act
        var result = await sut.ExecAsync(
            CreateJob(),
            TestContext.Current.CancellationToken);

        // Assert
        result.Should().BeTrue();

        s3.Verify(client => client.GetObjectAsync(
                ExternalBucketName,
                SourceKey,
                It.IsAny<CancellationToken>()),
            Times.Exactly(2));

        s3.Verify(client => client.PutObjectAsync(
                It.IsAny<PutObjectRequest>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task CopyWithRetryAsync_WhenAllAttemptsFail_ThrowsAfterMaximumRetries()
    {
        // Arrange
        var s3 = new Mock<IAmazonS3>();

        s3.Setup(client => client.GetObjectAsync(
                ExternalBucketName,
                SourceKey,
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonS3Exception("S3 unavailable"));

        var sut = GetSut(s3, new Mock<IAesCryptoTransform>());

        // Act
        await Assert.ThrowsAsync<AmazonS3Exception>(async () => await sut.ExecAsync(
            CreateJob(),
            TestContext.Current.CancellationToken));

        // Assert
        s3.Verify(client => client.GetObjectAsync(ExternalBucketName, SourceKey, It.IsAny<CancellationToken>()), Times.Exactly(3));
        s3.Verify(client => client.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CopyWithRetryAsync_WhenCancellationIsRequested_ReturnsFalseAndDoesNotCopy()
    {
        // Arrange
        using var cancellationTokenSource = new CancellationTokenSource();
        await cancellationTokenSource.CancelAsync();

        var s3 = new Mock<IAmazonS3>();
        var sut = GetSut(s3, new Mock<IAesCryptoTransform>());

        // Act
        var result = await sut.ExecAsync(
            CreateJob(),
            cancellationTokenSource.Token);

        // Assert
        result.Should().BeFalse();

        s3.Verify(client => client.PutObjectAsync(
                It.IsAny<PutObjectRequest>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task CopyWithRetryAsync_WhenRemoteFileMetadataIsNotFound_RetriesAndThrowsBecauseFileSizeIsInvalid()
    {
        // Arrange
        var s3 = CreateS3Mock(encryptedContent: "encrypted file content", contentLength: 1024);
        s3.Setup(client => client.GetObjectMetadataAsync(ExternalBucketName, SourceKey, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonS3Exception("Not found") { StatusCode = HttpStatusCode.NotFound });

        var sut = GetSut(s3, CreateAesCryptoTransformMock("decrypted"));

        // Act
        await Assert.ThrowsAsync<AmazonS3Exception>(async () =>
            await sut.ExecAsync(
                CreateJob(),
                TestContext.Current.CancellationToken));

        // Assert
        s3.Verify(client => client.GetObjectAsync(
                ExternalBucketName,
                SourceKey,
                It.IsAny<CancellationToken>()),
            Times.Exactly(3));

        s3.Verify(client => client.PutObjectAsync(
                It.IsAny<PutObjectRequest>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    private static S3CopyService GetSut(
        Mock<IAmazonS3> s3,
        Mock<IAesCryptoTransform> aesCryptoTransform)
    {
        var s3ClientFactory = new Mock<IS3ClientFactory>();

        s3ClientFactory
            .Setup(x => x.GetClientInfo<ExternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(s3.Object, ExternalBucketName));

        s3ClientFactory
            .Setup(x => x.GetClientInfo<InternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(s3.Object, InternalBucketName));

        var logger = new Mock<ILogger<S3CopyService>>();
        logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        return new S3CopyService(
            s3ClientFactory.Object,
            aesCryptoTransform.Object,
            s_transferUtilityAdapterMock.Object,
            logger.Object);
    }

    private static Mock<IAmazonS3> CreateS3Mock(
        string encryptedContent,
        long? contentLength = null)
    {
        var s3Mock = S3MockBuilder.Create(encryptedContent).S3;
        if (contentLength.HasValue)
        {
            s3Mock.Setup(client => client.GetObjectMetadataAsync(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => new GetObjectMetadataResponse
                {
                    ContentLength = contentLength.Value
                });
        }

        return s3Mock;
    }

    private static Mock<IAesCryptoTransform> CreateAesCryptoTransformMock(
        string decryptedContent)
    {
        var aesCryptoTransform = new Mock<IAesCryptoTransform>();

        aesCryptoTransform
            .Setup(x => x.DecryptStreamAsync(
                It.IsAny<Stream>(),
                It.IsAny<Stream>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<long?>(),
                It.IsAny<ProgressCallback?>(),
                It.IsAny<CancellationToken>()))
            .Returns<Stream, Stream, string, string, long?, ProgressCallback?, CancellationToken>(
                async (_, outputStream, _, _, _, _, cancellationToken) =>
                {
                    var bytes = Encoding.UTF8.GetBytes(decryptedContent);
                    await outputStream.WriteAsync(bytes, cancellationToken);
                    outputStream.Position = 0;
                });

        return aesCryptoTransform;
    }

    private static CsvDataFileImportJob CreateJob()
    {
        return new CsvDataFileImportJob(
            JobId: "job-1",
            SourceKey: SourceKey,
            TargetKey: TargetKey,
            Password: Password,
            Salt: Salt,
            SplitType: SplitType.None,
            SplitValue: null);
    }
}