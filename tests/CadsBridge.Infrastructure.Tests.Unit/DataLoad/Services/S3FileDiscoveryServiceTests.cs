using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Messages;
using CadsBridge.Application.Messaging.Models;
using CadsBridge.Application.Messaging.Publishers;
using CadsBridge.Core.ApiClients;
using CadsBridge.Core.Correlation;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.DTOs;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Configuration;
using CadsBridge.Infrastructure.Storage.Factories;
using FluentAssertions;
using Moq;
using System.Net;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services;

public class S3FileDiscoveryServiceTests
{
    private const string Bucket = "discovery-bucket";

    public class GetFileNamesTests
    {
        [Fact]
        public async Task GetFileNames_ReturnsEmpty_WhenBucketIsEmpty()
        {
            var sut = CreateSut(
            [
                MakePage(isTruncated: false)
            ]);

            var result = await sut.GetFileNames(null, TestContext.Current.CancellationToken);

            result.Should().BeEmpty();
        }

        [Fact]
        public async Task GetFileNames_ReturnsAllKeys_WhenSinglePage()
        {
            var sut = CreateSut(
            [
                MakePage(isTruncated: false, keys: ["file1.csv", "file2.csv", "file3.csv"])
            ]);

            var result = await sut.GetFileNames(null, TestContext.Current.CancellationToken);

            result.Should().BeEquivalentTo("file1.csv", "file2.csv", "file3.csv");
        }

        [Fact]
        public async Task GetFileNames_ReturnsAllKeys_WhenMultiplePages()
        {
            var s3 = new Mock<IAmazonS3>();
            var callCount = 0;

            s3.Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
              .ReturnsAsync(() =>
              {
                  callCount++;
                  return callCount == 1
                      ? MakePage(isTruncated: true, nextToken: "token1", "page1-file1.csv", "page1-file2.csv")
                      : MakePage(isTruncated: false, keys: ["page2-file1.csv"]);
              });

            var sut = CreateSut(s3.Object);

            var result = await sut.GetFileNames(null, TestContext.Current.CancellationToken);

            result.Should().BeEquivalentTo("page1-file1.csv", "page1-file2.csv", "page2-file1.csv");
        }

        [Fact]
        public async Task GetFileNames_SetsContinuationToken_BetweenPages()
        {
            var s3 = new Mock<IAmazonS3>();
            var capturedTokens = new List<string?>();
            var callCount = 0;

            s3.Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
                .Callback<ListObjectsV2Request, CancellationToken>((req, _) => capturedTokens.Add(req.ContinuationToken))
                .ReturnsAsync(() =>
                {
                    callCount++;
                    return callCount == 1
                        ? MakePage(isTruncated: true, nextToken: "my-continuation-token", keys: ["file1.csv"])
                        : MakePage(isTruncated: false, keys: ["file2.csv"]);
                });

            var sut = CreateSut(s3.Object);
            await sut.GetFileNames(null, TestContext.Current.CancellationToken);

            capturedTokens.Should().HaveCount(2);
            capturedTokens[0].Should().BeNull();
            capturedTokens[1].Should().Be("my-continuation-token");
        }

        [Fact]
        public async Task GetFileNames_UsesBucketName_FromClientInfo()
        {
            var s3 = new Mock<IAmazonS3>();
            ListObjectsV2Request? capturedRequest = null;

            s3.Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
              .Callback<ListObjectsV2Request, CancellationToken>((req, _) => capturedRequest = req)
              .ReturnsAsync(MakePage(isTruncated: false));

            var sut = CreateSut(s3.Object);
            await sut.GetFileNames(null, TestContext.Current.CancellationToken);

            capturedRequest.Should().NotBeNull();
            capturedRequest!.BucketName.Should().Be(Bucket);
        }
        [Fact]
        public async Task IsFileValid_ReturnsTrueWhenNotFound()
        {
            var fileApiService = new Mock<IFileImportApiService>();
            fileApiService.Setup(x => x.GetByFileNameIfExists(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileImportDto?)null);

            var sut = CreateSut(fileApiService.Object);
            var result = await sut.IsFileValid("test-file", TestContext.Current.CancellationToken);

            result.Should().BeTrue();
        }

        [Fact]
        public async Task IsFileValid_ReturnsTrueWhenFoundAndFailedLessThan3Times()
        {
            var fileApiService = new Mock<IFileImportApiService>();
            fileApiService.Setup(x => x.GetByFileNameIfExists(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileImportDto(2) { ImportStatus = FileImportStatus.Failed });

            var sut = CreateSut(fileApiService.Object);
            var result = await sut.IsFileValid("test-file", TestContext.Current.CancellationToken);

            result.Should().BeTrue();
        }

        [Fact]
        public async Task IsFileValid_ReturnsFalseWhenFoundAndFailedMoreThan2Times()
        {
            var fileApiService = new Mock<IFileImportApiService>();
            fileApiService.Setup(x => x.GetByFileNameIfExists(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileImportDto(3) { ImportStatus = FileImportStatus.Failed });

            var sut = CreateSut(fileApiService.Object);
            var result = await sut.IsFileValid("test-file", TestContext.Current.CancellationToken);

            result.Should().BeFalse();
        }

        private static S3FileDiscoveryService<ExternalStorageClient> CreateSut(
            IEnumerable<ListObjectsV2Response> pages)
        {
            var s3 = new Mock<IAmazonS3>();
            var queue = new Queue<ListObjectsV2Response>(pages);

            s3.Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
              .ReturnsAsync(queue.Dequeue);

            return CreateSut(s3.Object);
        }

        private static S3FileDiscoveryService<ExternalStorageClient> CreateSut(IAmazonS3 s3Client)
        {
            var factory = new Mock<IS3ClientFactory>();
            factory.Setup(x => x.GetClientInfo<ExternalStorageClient>())
                   .Returns(new S3ClientFactory.ClientInfo(s3Client, Bucket));
            var fileImportApiService = new Mock<IFileImportApiService>();
            return new S3FileDiscoveryService<ExternalStorageClient>(
                factory.Object,
                fileImportApiService.Object,
                Mock.Of<IMessagePublisher<CadsBridgeFifoQueueClient>>(),
                new StorageConfiguration());
        }

        private static S3FileDiscoveryService<ExternalStorageClient> CreateSut(IFileImportApiService fileImportApiService)
        {
            var factory = new Mock<IS3ClientFactory>();
            factory.Setup(x => x.GetClientInfo<ExternalStorageClient>())
                   .Returns(new S3ClientFactory.ClientInfo(Mock.Of<IAmazonS3>(), Bucket));
            return new S3FileDiscoveryService<ExternalStorageClient>(
                factory.Object,
                fileImportApiService,
                Mock.Of<IMessagePublisher<CadsBridgeFifoQueueClient>>(),
                new StorageConfiguration());
        }

        private static ListObjectsV2Response MakePage(
            bool isTruncated,
            string? nextToken = null,
            params string[] keys) =>
            new()
            {
                HttpStatusCode = HttpStatusCode.OK,
                IsTruncated = isTruncated,
                NextContinuationToken = nextToken,
                S3Objects = [.. keys.Select(k => new S3Object { Key = k })]
            };
    }

    public class EnQueueFileImportMessagesTests : IDisposable
    {
        private const string DestinationPrefix = "import/cts/bulk";
        private readonly Mock<IAmazonS3> _s3Mock = new();
        private readonly Mock<IMessagePublisher<CadsBridgeFifoQueueClient>> _publisherMock = new();

        public void Dispose()
        {
            CorrelationIdContext.Value = null;
            GC.SuppressFinalize(this);
        }

        private S3FileDiscoveryService<ExternalStorageClient> CreateSut(StorageConfiguration? storageConfiguration = null)
        {
            var factory = new Mock<IS3ClientFactory>();
            factory.Setup(x => x.GetClientInfo<ExternalStorageClient>())
                   .Returns(new S3ClientFactory.ClientInfo(_s3Mock.Object, Bucket));

            return new S3FileDiscoveryService<ExternalStorageClient>(
                factory.Object,
                Mock.Of<IFileImportApiService>(),
                _publisherMock.Object,
                storageConfiguration ?? new StorageConfiguration());
        }

        [Fact]
        public async Task EnQueueFileImportMessages_ShouldDoNothing_WhenFileNamesIsEmpty()
        {
            var sut = CreateSut();

            await sut.EnQueueFileImportMessages([], DestinationPrefix, TestContext.Current.CancellationToken);

            _s3Mock.Verify(x => x.GetObjectMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
            _publisherMock.Verify(x => x.PublishAsync(It.IsAny<object>(), It.IsAny<FifoMessageMetadata>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Theory]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData(null)]
        public async Task EnQueueFileImportMessages_ShouldThrow_WhenDestinationPrefixIsMissing(string? destinationPrefix)
        {
            var sut = CreateSut();

            var act = () => sut.EnQueueFileImportMessages(["file1.csv"], destinationPrefix!, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<ArgumentException>();
            _publisherMock.Verify(x => x.PublishAsync(It.IsAny<object>(), It.IsAny<FifoMessageMetadata>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task EnQueueFileImportMessages_ShouldPublishMessage_ForEachFileName()
        {
            _s3Mock
                .Setup(x => x.GetObjectMetadataAsync(Bucket, It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new GetObjectMetadataResponse { ETag = "\"etag-value\"" });

            var sut = CreateSut();

            await sut.EnQueueFileImportMessages(["file1.csv", "file2.csv"], DestinationPrefix, TestContext.Current.CancellationToken);

            _publisherMock.Verify(
                x => x.PublishAsync(
                    It.IsAny<CsvDataFileImportMessage>(),
                    It.IsAny<FifoMessageMetadata>(),
                    It.IsAny<CancellationToken>()),
                Times.Exactly(2));
        }

        [Fact]
        public async Task EnQueueFileImportMessages_ShouldPublishMessage_WithExpectedProperties()
        {
            const string fileName = "file1.csv";
            var storageConfiguration = new StorageConfiguration
            {
                External = new StorageConfigurationsDetailsWithCredentials { EnvironmentName = "PreProd" }
            };

            _s3Mock
                .Setup(x => x.GetObjectMetadataAsync(Bucket, fileName, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new GetObjectMetadataResponse { ETag = "\"etag-value\"" });

            CsvDataFileImportMessage? publishedMessage = null;
            FifoMessageMetadata publishedMetadata = default;

            _publisherMock
                .Setup(x => x.PublishAsync(It.IsAny<CsvDataFileImportMessage>(), It.IsAny<FifoMessageMetadata>(), It.IsAny<CancellationToken>()))
                .Callback<CsvDataFileImportMessage, FifoMessageMetadata, CancellationToken>((msg, meta, _) =>
                {
                    publishedMessage = msg;
                    publishedMetadata = meta;
                })
                .Returns(Task.CompletedTask);

            var sut = CreateSut(storageConfiguration);

            await sut.EnQueueFileImportMessages([fileName], DestinationPrefix, TestContext.Current.CancellationToken);

            publishedMessage.Should().NotBeNull();
            publishedMessage!.Bucket.Should().Be(Bucket);
            publishedMessage.ObjectKey.Should().Be(fileName);
            publishedMessage.DestinationPrefix.Should().Be(DestinationPrefix);
            publishedMessage.Etag.Should().Be("\"etag-value\"");
            publishedMessage.OracleEnvironment.Should().Be("PreProd");
            publishedMessage.Id.Should().NotBe(Guid.Empty);

            publishedMetadata.MessageGroupId.Should().Be($"{fileName}:PreProd");
            publishedMetadata.MessageDeduplicationId.Should().NotBeNullOrWhiteSpace();

            publishedMessage.CorrelationId.Should().Be(publishedMetadata.CorrelationId);
        }
    }
}