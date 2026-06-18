using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.Storage.Transfer;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.Crypto;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Factories;
using CadsBridge.Testing.Support.TestDoubles.Crypto;
using CadsBridge.Testing.Support.TestDoubles.Storage;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Text;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services;

public class S3CopyServiceTests
{
    private const string ExternalBucket = "external-bucket";
    private const string InternalBucket = "internal-bucket";
    private const string SourceKey = "incoming/source.csv";
    private const string TargetKey = "imported/source.csv";
    private const string Password = "pw";
    private const string Salt = "salt";

    [Fact]
    public async Task Small_file_uses_PutObject()
    {
        var s3 = new FakeS3
        {
            FileSize = 1024,
            EncryptedContent = "encrypted"
        };

        var aes = new FakeAesCryptoTransform("decrypted");
        var transfer = new FakeTransferUtilityAdapter();

        var sut = CreateSut(s3, aes, transfer);

        var result = await sut.ExecAsync(CreateJob(), CancellationToken.None);

        result.Should().BeTrue();
        s3.PutRequests.Should().ContainSingle();
        transfer.Uploads.Should().BeEmpty();
    }

    [Fact]
    public async Task Large_file_uses_multipart_upload()
    {
        var s3 = new FakeS3
        {
            FileSize = 150 * 1024 * 1024,
            EncryptedContent = "encrypted"
        };

        var aes = new FakeAesCryptoTransform("decrypted");
        var transfer = new FakeTransferUtilityAdapter();

        var sut = CreateSut(s3, aes, transfer);

        var result = await sut.ExecAsync(CreateJob(), CancellationToken.None);

        result.Should().BeTrue();
        transfer.Uploads.Should().ContainSingle();
        s3.PutRequests.Should().BeEmpty();
    }

    [Fact]
    public async Task Retries_when_GetObject_fails_once()
    {
        var attempts = 0;

        var s3 = new Mock<IAmazonS3>();

        s3.Setup(x => x.GetObjectAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
          .ReturnsAsync(() =>
          {
              attempts++;
              if (attempts == 1)
                  throw new AmazonS3Exception("fail");

              return new GetObjectResponse
              {
                  ResponseStream = new MemoryStream(Encoding.UTF8.GetBytes("encrypted"))
              };
          });

        s3.Setup(x => x.GetObjectMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
          .ReturnsAsync(new GetObjectMetadataResponse { ContentLength = 1024 });

        var aes = new FakeAesCryptoTransform("decrypted");
        var transfer = new FakeTransferUtilityAdapter();

        var sut = CreateSut(s3.Object, aes, transfer);

        var result = await sut.ExecAsync(CreateJob(), CancellationToken.None);

        result.Should().BeTrue();
        attempts.Should().Be(2);
    }

    [Fact]
    public async Task Cancels_cleanly()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var s3 = new FakeS3();
        var aes = new FakeAesCryptoTransform("ignored");
        var transfer = new FakeTransferUtilityAdapter();

        var sut = CreateSut(s3, aes, transfer);

        var result = await sut.ExecAsync(CreateJob(), cts.Token);

        result.Should().BeFalse();
        s3.PutRequests.Should().BeEmpty();
        transfer.Uploads.Should().BeEmpty();
    }

    [Fact]
    public async Task Throws_when_metadata_fails()
    {
        var s3 = new Mock<IAmazonS3>();

        s3.Setup(x => x.GetObjectAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
          .ReturnsAsync(new GetObjectResponse
          {
              ResponseStream = new MemoryStream(Encoding.UTF8.GetBytes("encrypted"))
          });

        s3.Setup(x => x.GetObjectMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
          .ThrowsAsync(new AmazonS3Exception("not found"));

        var aes = new FakeAesCryptoTransform("decrypted");
        var transfer = new FakeTransferUtilityAdapter();

        var sut = CreateSut(s3.Object, aes, transfer);

        await Assert.ThrowsAsync<AmazonS3Exception>(() =>
            sut.ExecAsync(CreateJob(), CancellationToken.None));
    }

    private static S3CopyService CreateSut(
        IAmazonS3 s3,
        IAesCryptoTransform aes,
        ITransferUtilityAdapter transfer)
    {
        var factory = new Mock<IS3ClientFactory>();

        factory.Setup(x => x.GetClientInfo<ExternalStorageClient>())
               .Returns(new S3ClientFactory.ClientInfo(s3, ExternalBucket));

        factory.Setup(x => x.GetClientInfo<InternalStorageClient>())
               .Returns(new S3ClientFactory.ClientInfo(s3, InternalBucket));

        var logger = new Mock<ILogger<S3CopyService>>();
        logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);

        return new S3CopyService(factory.Object, aes, transfer, logger.Object);
    }

    private static CsvDataFileImportJob CreateJob() =>
        new(
            JobId: "job-1",
            SourceKey: SourceKey,
            TargetKey: TargetKey,
            Password: Password,
            Salt: Salt,
            SplitType: SplitType.None,
            SplitValue: null);
}