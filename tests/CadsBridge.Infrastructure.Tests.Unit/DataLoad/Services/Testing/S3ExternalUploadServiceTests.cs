using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Crypto;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Infrastructure.DataLoad.Services.Testing;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Factories;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;
using CadsBridge.Application.Extensions;
using CadsBridge.Core.Attributes;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services.Testing;

public class S3ExternalUploadServiceTests
{
    private const string Bucket = "external-bucket";
    private const string Key = "CTSM_CADS_TEST_FULL_BATCH1_MYTABLE_2026-07-10-120000.csv";
    private const string Salt = "salt";

    [Fact]
    public async Task Uploads_successfully_when_file_does_not_exist()
    {
        var s3 = new Mock<IAmazonS3>();

        s3.Setup(x => x.GetObjectMetadataAsync(Bucket, Key, It.IsAny<CancellationToken>()))
          .ThrowsAsync(new AmazonS3Exception("not found") { StatusCode = HttpStatusCode.NotFound });

        s3.Setup(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
          .ReturnsAsync(new PutObjectResponse());

        var aes = new Mock<IAesCryptoTransform>();
        aes.Setup(x => x.EncryptStreamAsync(
                It.IsAny<Stream>(), It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<long?>(), It.IsAny<ProgressCallback?>(), It.IsAny<CancellationToken>()))
           .Returns((Stream input, Stream output, string _, string _, long? _, ProgressCallback? _, CancellationToken _) =>
           {
               input.CopyTo(output);
               output.Position = 0;
               return Task.CompletedTask;
           });

        var sut = CreateSut(s3.Object, aes.Object);

        var result = await sut.UploadAsync(Key, "some data", DataSourceType.CtsBulk, overwriteExisting: false, CancellationToken.None);

        var prefix = DataSourceType.CtsBulk.GetAttribute<DataSourceTypeInfoAttribute>()!.Prefix;
        result.Key.Should().Be($"{prefix}/{Key}");
        result.BucketName.Should().Be(Bucket);
        result.Size.Should().BeGreaterThan(0);

        s3.Verify(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Overwrites_existing_file_when_overwriteExisting_is_true()
    {
        var s3 = new Mock<IAmazonS3>();

        s3.Setup(x => x.GetObjectMetadataAsync(Bucket, Key, It.IsAny<CancellationToken>()))
          .ReturnsAsync(new GetObjectMetadataResponse());

        s3.Setup(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
          .ReturnsAsync(new PutObjectResponse());

        var aes = new Mock<IAesCryptoTransform>();
        aes.Setup(x => x.EncryptStreamAsync(
                It.IsAny<Stream>(), It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<long?>(), It.IsAny<ProgressCallback?>(), It.IsAny<CancellationToken>()))
           .Returns(Task.CompletedTask);

        var sut = CreateSut(s3.Object, aes.Object);

        var result = await sut.UploadAsync(Key, "some data", DataSourceType.CtsBulk, overwriteExisting: true, CancellationToken.None);

        var prefix = DataSourceType.CtsBulk.GetAttribute<DataSourceTypeInfoAttribute>()!.Prefix;
        result.Key.Should().Be($"{prefix}/{Key}");
        s3.Verify(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Throws_ConflictException_when_file_exists_and_overwriteExisting_is_false()
    {
        var s3 = new Mock<IAmazonS3>();

        s3.Setup(x => x.GetObjectMetadataAsync(Bucket, Key, It.IsAny<CancellationToken>()))
          .ReturnsAsync(new GetObjectMetadataResponse());

        var aes = new Mock<IAesCryptoTransform>();

        var sut = CreateSut(s3.Object, aes.Object);

        await Assert.ThrowsAsync<ConflictException>(() =>
            sut.UploadAsync(Key, "some data", DataSourceType.CtsBulk, overwriteExisting: false, CancellationToken.None));

        s3.Verify(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Rethrows_non_NotFound_AmazonS3Exception_from_metadata_check()
    {
        var s3 = new Mock<IAmazonS3>();

        s3.Setup(x => x.GetObjectMetadataAsync(Bucket, Key, It.IsAny<CancellationToken>()))
          .ThrowsAsync(new AmazonS3Exception("forbidden") { StatusCode = HttpStatusCode.Forbidden });

        var aes = new Mock<IAesCryptoTransform>();

        var sut = CreateSut(s3.Object, aes.Object);

        await Assert.ThrowsAsync<AmazonS3Exception>(() =>
            sut.UploadAsync(Key, "some data", DataSourceType.CtsBulk, overwriteExisting: false, CancellationToken.None));
    }

    [Fact]
    public async Task Throws_and_logs_when_encryption_fails()
    {
        var s3 = new Mock<IAmazonS3>();

        s3.Setup(x => x.GetObjectMetadataAsync(Bucket, Key, It.IsAny<CancellationToken>()))
          .ThrowsAsync(new AmazonS3Exception("not found") { StatusCode = HttpStatusCode.NotFound });

        var aes = new Mock<IAesCryptoTransform>();
        aes.Setup(x => x.EncryptStreamAsync(
                It.IsAny<Stream>(), It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<long?>(), It.IsAny<ProgressCallback?>(), It.IsAny<CancellationToken>()))
           .ThrowsAsync(new InvalidOperationException("encryption failure"));

        var logger = new Mock<ILogger<S3ExternalUploadService<ExternalStorageClient>>>();
        logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);

        var sut = CreateSut(s3.Object, aes.Object, logger);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            sut.UploadAsync(Key, "some data", DataSourceType.CtsBulk, overwriteExisting: false, CancellationToken.None));

        logger.Verify(x => x.Log(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.IsAny<It.IsAnyType>(),
            It.IsAny<InvalidOperationException>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);

        s3.Verify(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Throws_and_logs_when_upload_fails()
    {
        var s3 = new Mock<IAmazonS3>();

        s3.Setup(x => x.GetObjectMetadataAsync(Bucket, Key, It.IsAny<CancellationToken>()))
          .ThrowsAsync(new AmazonS3Exception("not found") { StatusCode = HttpStatusCode.NotFound });

        s3.Setup(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
          .ThrowsAsync(new AmazonS3Exception("upload failure"));

        var aes = new Mock<IAesCryptoTransform>();
        aes.Setup(x => x.EncryptStreamAsync(
                It.IsAny<Stream>(), It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<long?>(), It.IsAny<ProgressCallback?>(), It.IsAny<CancellationToken>()))
           .Returns(Task.CompletedTask);

        var logger = new Mock<ILogger<S3ExternalUploadService<ExternalStorageClient>>>();
        logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);

        var sut = CreateSut(s3.Object, aes.Object, logger);

        await Assert.ThrowsAsync<AmazonS3Exception>(() =>
            sut.UploadAsync(Key, "some data", DataSourceType.CtsBulk, overwriteExisting: false, CancellationToken.None));

        logger.Verify(x => x.Log(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.IsAny<It.IsAnyType>(),
            It.IsAny<AmazonS3Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    private static S3ExternalUploadService<ExternalStorageClient> CreateSut(
        IAmazonS3 s3,
        IAesCryptoTransform aes,
        Mock<ILogger<S3ExternalUploadService<ExternalStorageClient>>>? logger = null)
    {
        var factory = new Mock<IS3ClientFactory>();

        factory.Setup(x => x.GetClientInfo<ExternalStorageClient>())
               .Returns(new S3ClientFactory.ClientInfo(s3, Bucket));

        logger ??= new Mock<ILogger<S3ExternalUploadService<ExternalStorageClient>>>();
        logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);

        var config = new DataLoadConfiguration { Salt = Salt };

        return new S3ExternalUploadService<ExternalStorageClient>(factory.Object, aes, config, logger.Object);
    }
}