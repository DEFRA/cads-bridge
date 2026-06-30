using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Factories;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;
using System.Text;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services;

public class S3FileMetaDataServiceTests
{
    // ── Pure static helper: ExtractLastLine ───────────────────────────────────

    public class ExtractLastLineTests
    {
        [Fact]
        public void ExtractLastLine_ReturnsLastLine_WhenUnixLineEndings()
        {
            var stream = ToStream("line1\nline2\nT|file.csv|13092020 17:59:11|999");

            S3FileMetaDataService.ExtractLastLine(stream)
                .Should().Be("T|file.csv|13092020 17:59:11|999");
        }

        [Fact]
        public void ExtractLastLine_ReturnsLastLine_WhenWindowsLineEndings()
        {
            var stream = ToStream("line1\r\nline2\r\nT|file.csv|13092020 17:59:11|999");

            S3FileMetaDataService.ExtractLastLine(stream)
                .Should().Be("T|file.csv|13092020 17:59:11|999");
        }

        [Fact]
        public void ExtractLastLine_ReturnsLastLine_WhenTrailingUnixNewlinePresent()
        {
            var stream = ToStream("line1\nT|file.csv|13092020 17:59:11|999\n");

            S3FileMetaDataService.ExtractLastLine(stream)
                .Should().Be("T|file.csv|13092020 17:59:11|999");
        }

        [Fact]
        public void ExtractLastLine_ReturnsLastLine_WhenTrailingCrLfPresent()
        {
            var stream = ToStream("line1\r\nT|file.csv|13092020 17:59:11|999\r\n");

            S3FileMetaDataService.ExtractLastLine(stream)
                .Should().Be("T|file.csv|13092020 17:59:11|999");
        }

        [Fact]
        public void ExtractLastLine_ReturnsSingleLine_WhenNoLineBreaks()
        {
            var stream = ToStream("T|file.csv|13092020 17:59:11|42");

            S3FileMetaDataService.ExtractLastLine(stream)
                .Should().Be("T|file.csv|13092020 17:59:11|42");
        }

        [Fact]
        public void ExtractLastLine_ReturnsEmpty_WhenStreamContainsOnlyNewlines()
        {
            var stream = ToStream("\n\r\n\n");

            S3FileMetaDataService.ExtractLastLine(stream)
                .Should().BeEmpty();
        }

        private static Stream ToStream(string text)
            => new MemoryStream(Encoding.UTF8.GetBytes(text));
    }

    // ── Instance method: ParseTrailerLine ─────────────────────────────────────

    public class ParseTrailerLineTests
    {
        private const string Bucket = "internal-bucket";

        [Fact]
        public void ParseTrailerLine_ReturnsRecordCount_WhenLineIsValid()
        {
            CreateSut()
                .ParseTrailerLine("T|file.csv|13092020 17:59:11|1234567", "imports/file.csv")
                .Should().Be(1234567L);
        }

        [Fact]
        public void ParseTrailerLine_ReturnsZero_WhenRecordCountIsZero()
        {
            CreateSut()
                .ParseTrailerLine("T|file.csv|13092020 17:59:11|0", "imports/file.csv")
                .Should().Be(0L);
        }

        [Theory]
        [InlineData("T|file.csv|13092020 17:59:11")]           // 3 fields
        [InlineData("T|file.csv|13092020 17:59:11|100|extra")] // 5 fields
        public void ParseTrailerLine_Throws_WhenFieldCountIsWrong(string line)
        {
            var sut = CreateSut();

            var act = () => sut.ParseTrailerLine(line, "imports/file.csv");

            act.Should().Throw<DomainException>().WithMessage("*field(s)*");
        }

        [Fact]
        public void ParseTrailerLine_Throws_WhenFirstFieldIsNotT()
        {
            var sut = CreateSut();

            var act = () => sut.ParseTrailerLine("H|file.csv|13092020 17:59:11|100", "imports/file.csv");

            act.Should().Throw<DomainException>().WithMessage("*does not begin with 'T'*");
        }

        [Fact]
        public void ParseTrailerLine_Throws_WhenFileNameDoesNotMatch()
        {
            var sut = CreateSut();

            var act = () => sut.ParseTrailerLine("T|other.csv|13092020 17:59:11|100", "imports/file.csv");

            act.Should().Throw<DomainException>().WithMessage("*does not match expected*");
        }

        [Theory]
        [InlineData("T|file.csv|13092020 17:59:11|abc")]
        [InlineData("T|file.csv|13092020 17:59:11|-1")]
        [InlineData("T|file.csv|13092020 17:59:11|")]
        public void ParseTrailerLine_Throws_WhenRecordCountIsInvalid(string line)
        {
            var sut = CreateSut();

            var act = () => sut.ParseTrailerLine(line, "imports/file.csv");

            act.Should().Throw<DomainException>().WithMessage("*not a valid non-negative integer*");
        }

        [Fact]
        public void ParseTrailerLine_IsCaseInsensitive_ForFileName()
        {
            CreateSut()
                .ParseTrailerLine("T|FILE.CSV|13092020 17:59:11|50", "imports/file.csv")
                .Should().Be(50L);
        }

        [Fact]
        public void ParseTrailerLine_LogsError_WhenLineIsInvalid()
        {
            var logger = new Mock<ILogger<S3FileMetaDataService>>();
            logger.Setup(x => x.IsEnabled(LogLevel.Error)).Returns(true);

            var sut = CreateSut(logger);

            var act = () => sut.ParseTrailerLine("H|file.csv|13092020 17:59:11|100", "imports/file.csv");

            act.Should().Throw<DomainException>();

            logger.Verify(
                x => x.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.IsAny<It.IsAnyType>(),
                    It.IsAny<Exception?>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        private static S3FileMetaDataService CreateSut(Mock<ILogger<S3FileMetaDataService>>? logger = null)
        {
            var factory = new Mock<IS3ClientFactory>();
            factory.Setup(x => x.GetClientInfo<InternalStorageClient>())
                   .Returns(new S3ClientFactory.ClientInfo(Mock.Of<IAmazonS3>(), Bucket));

            return new S3FileMetaDataService(
                factory.Object,
                logger?.Object ?? Mock.Of<ILogger<S3FileMetaDataService>>());
        }
    }

    // ── GetRecordCountAsync (S3 fully mocked) ─────────────────────────────────

    public class GetRecordCountAsyncTests
    {
        private const string Bucket   = "internal-bucket";
        private const string S3Key    = "imports/data/myfile.csv";
        private const string FileName = "myfile.csv";

        [Fact]
        public async Task GetRecordCountAsync_ReturnsCount_WhenTrailerIsValid()
        {
            var sut = CreateSut(
                fileSize: 500,
                tailContent: $"T|{FileName}|13092020 17:59:11|9876543");

            var result = await sut.GetRecordCountAsync(S3Key, TestContext.Current.CancellationToken);

            result.Should().Be(9876543L);
        }

        [Fact]
        public async Task GetRecordCountAsync_Throws_NotFoundException_WhenMetadataReturns404()
        {
            var s3 = new Mock<IAmazonS3>();
            s3.Setup(x => x.GetObjectMetadataAsync(
                    It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
              .ThrowsAsync(new AmazonS3Exception("Not Found") { StatusCode = HttpStatusCode.NotFound });

            var act = async () =>
                await CreateSut(s3.Object)
                    .GetRecordCountAsync(S3Key, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NotFoundException>().WithMessage($"*'{S3Key}'*");
        }

        [Fact]
        public async Task GetRecordCountAsync_Throws_DomainException_WhenFileIsEmpty()
        {
            var sut = CreateSut(fileSize: 0, tailContent: string.Empty);

            var act = async () =>
                await sut.GetRecordCountAsync(S3Key, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<DomainException>().WithMessage("*empty*");
        }

        [Fact]
        public async Task GetRecordCountAsync_Throws_DomainException_WhenTrailerLineMalformed()
        {
            var sut = CreateSut(fileSize: 200, tailContent: "not|a|valid|trailer|line");

            var act = async () =>
                await sut.GetRecordCountAsync(S3Key, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<DomainException>();
        }

        [Fact]
        public async Task GetRecordCountAsync_Throws_DomainException_WhenFileNameMismatch()
        {
            var sut = CreateSut(fileSize: 200, tailContent: "T|wrongfile.csv|13092020 17:59:11|100");

            var act = async () =>
                await sut.GetRecordCountAsync(S3Key, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<DomainException>().WithMessage("*does not match expected*");
        }

        [Fact]
        public async Task GetRecordCountAsync_UsesRangedGet_ForLargeFile()
        {
            const long largeFileSize = 6L * 1024 * 1024 * 1024; // 6 GB

            var s3 = new Mock<IAmazonS3>();

            s3.Setup(x => x.GetObjectMetadataAsync(Bucket, S3Key, It.IsAny<CancellationToken>()))
              .ReturnsAsync(new GetObjectMetadataResponse { ContentLength = largeFileSize });

            GetObjectRequest? capturedRequest = null;
            s3.Setup(x => x.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
              .Callback<GetObjectRequest, CancellationToken>((req, _) => capturedRequest = req)
              .ReturnsAsync(new GetObjectResponse
              {
                  ResponseStream = new MemoryStream(
                      Encoding.UTF8.GetBytes($"T|{FileName}|13092020 17:59:11|1000000"))
              });

            await CreateSut(s3.Object)
                .GetRecordCountAsync(S3Key, TestContext.Current.CancellationToken);

            capturedRequest.Should().NotBeNull();
            capturedRequest!.ByteRange.Should().NotBeNull();
            capturedRequest.ByteRange.Start.Should().BeGreaterThan(0);
            capturedRequest.ByteRange.End.Should().Be(largeFileSize - 1);
        }

        [Fact]
        public async Task GetRecordCountAsync_Throws_NotFoundException_WhenGetObjectReturns404()
        {
            var s3 = new Mock<IAmazonS3>();

            s3.Setup(x => x.GetObjectMetadataAsync(Bucket, S3Key, It.IsAny<CancellationToken>()))
              .ReturnsAsync(new GetObjectMetadataResponse { ContentLength = 500 });

            s3.Setup(x => x.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
              .ThrowsAsync(new AmazonS3Exception("Not Found") { StatusCode = HttpStatusCode.NotFound });

            var act = async () =>
                await CreateSut(s3.Object)
                    .GetRecordCountAsync(S3Key, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NotFoundException>();
        }

        // ── helpers ──────────────────────────────────────────────────────────

        private static S3FileMetaDataService CreateSut(long fileSize, string tailContent)
        {
            var s3 = new Mock<IAmazonS3>();

            s3.Setup(x => x.GetObjectMetadataAsync(Bucket, S3Key, It.IsAny<CancellationToken>()))
              .ReturnsAsync(new GetObjectMetadataResponse { ContentLength = fileSize });

            s3.Setup(x => x.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
              .ReturnsAsync(new GetObjectResponse
              {
                  ResponseStream = new MemoryStream(Encoding.UTF8.GetBytes(tailContent))
              });

            return CreateSut(s3.Object);
        }

        private static S3FileMetaDataService CreateSut(IAmazonS3 s3Client)
        {
            var factory = new Mock<IS3ClientFactory>();
            factory.Setup(x => x.GetClientInfo<InternalStorageClient>())
                   .Returns(new S3ClientFactory.ClientInfo(s3Client, Bucket));

            return new S3FileMetaDataService(
                factory.Object,
                Mock.Of<ILogger<S3FileMetaDataService>>());
        }
    }
}
