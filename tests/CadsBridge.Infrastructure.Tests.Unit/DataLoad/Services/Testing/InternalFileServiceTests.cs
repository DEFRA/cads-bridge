using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Infrastructure.DataLoad.Services.Testing;
using CadsBridge.Infrastructure.Encryption;
using CadsBridge.Infrastructure.Encryption.Configuration;
using CadsBridge.Infrastructure.Encryption.Providers;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Factories;
using CadsBridge.Testing.Support.Utilities.Encryption;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Net;
using System.Text;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services.Testing;

public class InternalFileServiceTests
{
    private const string Bucket = "internal-bucket";
    private const string FileName = "CTSM_CADS_ETE_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.csv";
    private const string Stem = "CTSM_CADS_ETE_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638";
    private const string ChunkPrefix = $"import/cts/bulk/{Stem}/";

    private static string ChunkKey(int chunkNo) => $"{ChunkPrefix}{Stem}-part-{chunkNo:D4}.csv";

    private readonly Mock<IAmazonS3> _s3 = new();
    private readonly Dictionary<string, byte[]> _objects = [];

    private readonly FileEncryptor _encryptor = new(
        new InMemoryMasterKeyProvider(TestEncryptionKeyProvider.CurrentKeyId, TestEncryptionKeyProvider.Keys));

    public InternalFileServiceTests()
    {
        _s3.Setup(x => x.GetObjectAsync(Bucket, It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string _, string key, CancellationToken _) => new GetObjectResponse
            {
                ResponseStream = new MemoryStream(_objects[key])
            });
    }

    [Fact]
    public async Task ListChunksAsync_ReturnsPartsInFolder_OrderedByChunkNo_AcrossPages()
    {
        _objects[ChunkKey(1)] = Encode(Chunk(5));
        _objects[ChunkKey(2)] = Encode(Chunk(5));
        _objects[ChunkKey(3)] = Encode(Chunk(1));

        SetupListing(
            new ListObjectsV2Response
            {
                HttpStatusCode = HttpStatusCode.OK,
                IsTruncated = true,
                NextContinuationToken = "page-2",
                S3Objects =
                [
                    new S3Object { Key = ChunkKey(3), Size = 30 },
                    new S3Object { Key = $"{ChunkPrefix}unrelated.csv", Size = 1 }
                ]
            },
            new ListObjectsV2Response
            {
                HttpStatusCode = HttpStatusCode.OK,
                S3Objects =
                [
                    new S3Object { Key = ChunkKey(1), Size = 10 },
                    new S3Object { Key = ChunkKey(2), Size = 20 }
                ]
            });

        var sut = CreateSut(encryptionEnabled: false);

        var chunks = await sut.ListChunksAsync(FileName, TestContext.Current.CancellationToken);

        chunks.Select(c => c.ChunkNo).Should().Equal(1, 2, 3);
        chunks.Select(c => c.Key).Should().Equal(ChunkKey(1), ChunkKey(2), ChunkKey(3));
        chunks.Select(c => c.SizeBytes).Should().Equal(10, 20, 30);
        chunks.Select(c => c.RecordCount).Should().Equal(5, 5, 1);

        _s3.Verify(x => x.ListObjectsV2Async(
            It.Is<ListObjectsV2Request>(r => r.BucketName == Bucket && r.Prefix == ChunkPrefix && r.Delimiter == "/"),
            It.IsAny<CancellationToken>()), Times.Exactly(2));
        _s3.Verify(x => x.GetObjectAsync(Bucket, It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
    }

    [Fact]
    public async Task ListChunksAsync_WithEncryptionEnabled_DecryptsAndCountsRecords()
    {
        _objects[ChunkKey(1)] = await EncryptAsync(Chunk(3));
        _objects[ChunkKey(2)] = await EncryptAsync(Chunk(2));
        SetupListing(Page(ChunkKey(1), ChunkKey(2)));

        var sut = CreateSut(encryptionEnabled: true);

        var chunks = await sut.ListChunksAsync(FileName, TestContext.Current.CancellationToken);

        chunks.Select(c => c.RecordCount).Should().Equal(3, 2);
    }

    [Fact]
    public async Task ListChunksAsync_WithEncryptionEnabled_LargeChunk_StreamsWithoutDeadlock()
    {
        _objects[ChunkKey(1)] = await EncryptAsync(Chunk(50_000));
        SetupListing(Page(ChunkKey(1)));

        var sut = CreateSut(encryptionEnabled: true);

        var chunks = await sut.ListChunksAsync(FileName, TestContext.Current.CancellationToken);

        chunks.Single().RecordCount.Should().Be(50_000);
    }

    [Fact]
    public async Task ListChunksAsync_WithEncryptionEnabled_AndUnreadableChunk_Throws()
    {
        _objects[ChunkKey(1)] = Encode(Chunk(3));
        SetupListing(Page(ChunkKey(1)));

        var sut = CreateSut(encryptionEnabled: true);

        var act = () => sut.ListChunksAsync(FileName, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public async Task ListChunksAsync_WithNoObjects_ReturnsEmpty()
    {
        SetupListing(new ListObjectsV2Response { HttpStatusCode = HttpStatusCode.OK });

        var sut = CreateSut(encryptionEnabled: false);

        var chunks = await sut.ListChunksAsync(FileName, TestContext.Current.CancellationToken);

        chunks.Should().BeEmpty();
    }

    [Fact]
    public async Task ListChunksAsync_WithNonE2eFileName_ThrowsFormatException_WithoutCallingS3()
    {
        var sut = CreateSut(encryptionEnabled: false);

        var act = () => sut.ListChunksAsync(
            "CTSM_UKV_PROD_BULK_123456_1_CT_REGISTERED_MOVEMENTS_2026-02-22-074603.csv",
            TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<FormatException>();
        _s3.VerifyNoOtherCalls();
    }

    [Theory]
    [InlineData(FileName, $"import/cts/bulk/{Stem}/")]
    [InlineData("CTSM_CADS_ETE_DELTA_00002_001_CT_LOCATIONS_2026-07-28-094638.csv", "import/cts/daily/CTSM_CADS_ETE_DELTA_00002_001_CT_LOCATIONS_2026-07-28-094638/")]
    [InlineData("CTSM_CADS_ETE_BULK_00003_CT_LOCATIONS_2026-07-28-094638.csv", "import/cts/bulk/CTSM_CADS_ETE_BULK_00003_CT_LOCATIONS_2026-07-28-094638/")]
    public void GetPartsFolder_ReturnsSplitFolderForFile(string fileName, string expected)
    {
        InternalFileService.GetPartsFolder(fileName).Should().Be(expected);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not-a-ctsm-file.csv")]
    [InlineData("CTSM_UKV_PROD_BULK_123456_1_CT_REGISTERED_MOVEMENTS_2026-02-22-074603.csv")]
    [InlineData("CTSM_CADS_TEST_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.csv")]
    [InlineData("CTSM_UKV_ETE_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.csv")]
    [InlineData("CTSM_cads_ete_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.csv")]
    [InlineData("CTSM_CADS_ETE_FULL_00001_001_CT_LOCATIONS_2026-07-28-094638.csv")]
    [InlineData($"cads/cts/bulk/{FileName}")]
    [InlineData($"..\\{FileName}")]
    [InlineData($"{Stem}/../{FileName}")]
    [InlineData($" {FileName}")]
    public void GetPartsFolder_WithInvalidOrNonE2eName_ThrowsFormatException(string? fileName)
    {
        var act = () => InternalFileService.GetPartsFolder(fileName!);

        act.Should().Throw<FormatException>();
    }

    [Fact]
    public async Task ListChunksAsync_WhenListingFails_Throws()
    {
        SetupListing(new ListObjectsV2Response { HttpStatusCode = HttpStatusCode.InternalServerError });

        var sut = CreateSut(encryptionEnabled: false);

        var act = () => sut.ListChunksAsync(FileName, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task CountDataRecordsAsync_CountsOnlyDRecords()
    {
        var content = string.Join("\r\n",
            "HEADER|ignored",
            "record_type|column_one",
            "D|1|One",
            "",
            "d|lowercase is not a record",
            "DX|not a record",
            "D|2|Two",
            "T|file.csv|01012000 00:00:00|2");

        var bytes = Encoding.UTF8.GetPreamble().Concat(Encoding.UTF8.GetBytes("D|0|Zero\n" + content)).ToArray();

        var count = await InternalFileService.CountDataRecordsAsync(
            new MemoryStream(bytes), TestContext.Current.CancellationToken);

        count.Should().Be(3);
    }

    private InternalFileService CreateSut(bool encryptionEnabled)
    {
        var factory = new S3ClientFactory();
        factory.RegisterMockClient<InternalStorageClient>(Bucket, _s3.Object);

        var options = Options.Create(new EncryptionConfig
        {
            CurrentKeyId = TestEncryptionKeyProvider.CurrentKeyId,
            Keys = new Dictionary<string, string> { [TestEncryptionKeyProvider.CurrentKeyId] = TestEncryptionKeyProvider.CurrentKeyBase64 },
            StorageEncryptionEnabled = encryptionEnabled
        });

        return new InternalFileService(
            factory,
            _encryptor,
            options,
            NullLogger<InternalFileService>.Instance);
    }

    private void SetupListing(params ListObjectsV2Response[] pages)
    {
        var sequence = _s3.SetupSequence(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()));
        foreach (var page in pages)
        {
            sequence = sequence.ReturnsAsync(page);
        }
    }

    private static ListObjectsV2Response Page(params string[] keys) => new()
    {
        HttpStatusCode = HttpStatusCode.OK,
        S3Objects = [.. keys.Select(k => new S3Object { Key = k, Size = 1 })]
    };

    private static string Chunk(int records)
    {
        var builder = new StringBuilder();
        builder.AppendLine("record_type|column_one|column_two");
        for (var i = 1; i <= records; i++)
        {
            builder.AppendLine($"D|{i}|Value {i}");
        }

        return builder.ToString();
    }

    private static byte[] Encode(string content) => Encoding.UTF8.GetBytes(content);

    private async Task<byte[]> EncryptAsync(string content)
    {
        using var input = new MemoryStream(Encode(content));
        using var output = new MemoryStream();
        await _encryptor.EncryptAsync(input, output, cancellationToken: TestContext.Current.CancellationToken);
        return output.ToArray();
    }
}
