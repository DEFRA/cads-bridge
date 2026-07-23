using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Factories;
using FluentAssertions;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services;

public class S3FileDiscoveryServiceTests
{
    private const string Bucket = "discovery-bucket";

    public class GetFileNamesTests
    {
        [Fact]
        public async Task GetFileNames_ReturnsEmpty_WhenBucketIsEmpty()
        {
            var sut = CreateSut(new[]
            {
                MakePage(isTruncated: false)
            });

            var result = await sut.GetFileNames(TestContext.Current.CancellationToken);

            result.Should().BeEmpty();
        }

        [Fact]
        public async Task GetFileNames_ReturnsAllKeys_WhenSinglePage()
        {
            var sut = CreateSut(new[]
            {
                MakePage(isTruncated: false, keys: ["file1.csv", "file2.csv", "file3.csv"])
            });

            var result = await sut.GetFileNames(TestContext.Current.CancellationToken);

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

            var result = await sut.GetFileNames(TestContext.Current.CancellationToken);

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
            await sut.GetFileNames(TestContext.Current.CancellationToken);

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
            await sut.GetFileNames(TestContext.Current.CancellationToken);

            capturedRequest.Should().NotBeNull();
            capturedRequest!.BucketName.Should().Be(Bucket);
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
            return new S3FileDiscoveryService<ExternalStorageClient>(factory.Object, fileImportApiService.Object);
        }

        private static ListObjectsV2Response MakePage(
            bool isTruncated,
            string? nextToken = null,
            params string[] keys) =>
            new()
            {
                IsTruncated = isTruncated,
                NextContinuationToken = nextToken,
                S3Objects = keys.Select(k => new S3Object { Key = k }).ToList()
            };
    }
}