using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.DataLoad.Csv.Contracts;
using CadsBridge.Infrastructure.DataLoad.Csv.Factories;
using CadsBridge.Infrastructure.DataLoad.Csv.Strategies;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Testing.Support.TestDoubles.Storage;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services;

public class CsvDataFileSplitterServiceTests
{
    private const string BucketName = "internal-bucket";
    private const string SourceKey = "imports/source-file.csv";
    private const string DestinationPrefix = "split-output";

    private static string SmallInputFile => string.Join(
        Environment.NewLine,
        "HEADER|ignored",
        "C|RECORD_TYPE|COLUMN_ONE|COLUMN_TWO",
        "D|1|One") + Environment.NewLine;

    [Fact]
    public async Task Throws_when_split_value_is_null_for_by_lines()
    {
        var sut = CreateSut(new FakeS3());
        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, null);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            sut.ExecuteAsync(request, CancellationToken.None));
    }

    [Fact]
    public async Task Throws_when_split_value_is_null_for_by_size()
    {
        var sut = CreateSut(new FakeS3());
        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.BySize, null);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            sut.ExecuteAsync(request, CancellationToken.None));
    }

    [Fact]
    public async Task Throws_when_split_type_is_invalid()
    {
        var sut = CreateSut(new FakeS3());
        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, (SplitType)999, 2);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            sut.ExecuteAsync(request, CancellationToken.None));
    }

    [Fact]
    public async Task Copies_whole_file_as_single_part_when_split_type_none()
    {
        var s3 = new FakeS3 { EncryptedContent = SmallInputFile };
        var sut = CreateSut(s3);

        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.None, null);

        var result = await sut.ExecuteAsync(request, CancellationToken.None);

        result.Should().BeTrue();
        s3.UploadedContent.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["split-output/source-file-part-0001.csv"] = SmallInputFile
        });
    }

    [Fact]
    public async Task Retries_when_file_fails_to_load()
    {
        var s3 = new Mock<IAmazonS3>();

        s3.Setup(x => x.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
          .Throws<NullReferenceException>();

        var sut = CreateSut(s3.Object);

        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, 2);

        await Assert.ThrowsAsync<NullReferenceException>(() =>
            sut.ExecuteAsync(request, CancellationToken.None));

        s3.Verify(x => x.GetObjectAsync(
            It.Is<GetObjectRequest>(r => r.BucketName == BucketName && r.Key == SourceKey),
            It.IsAny<CancellationToken>()),
            Times.Exactly(3));
    }

    [Fact]
    public async Task Splits_file_by_lines_into_correct_chunks()
    {
        var sourceContent = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "C|RECORD_TYPE|FIRST_NAME|LAST_NAME",
            "D|Alice|Smith",
            "D|Bob|Jones",
            "D|Charlie|Brown",
            "D|Dana|White") + Environment.NewLine;

        var s3 = new FakeS3 { EncryptedContent = sourceContent };
        var sut = CreateSut(s3);

        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, 2);

        var result = await sut.ExecuteAsync(request, CancellationToken.None);

        result.Should().BeTrue();

        s3.UploadedContent.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["split-output/source-file-part-0001.csv"] = string.Join(
                Environment.NewLine,
                "record_type|first_name|last_name",
                "D|Alice|Smith",
                "D|Bob|Jones",
                string.Empty),
            ["split-output/source-file-part-0002.csv"] = string.Join(
                Environment.NewLine,
                "record_type|first_name|last_name",
                "D|Charlie|Brown",
                "D|Dana|White",
                string.Empty)
        });
    }

    [Fact]
    public async Task Does_not_upload_when_file_has_no_data_lines()
    {
        var sourceContent = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "C|RECORD_TYPE|COLUMN_ONE|COLUMN_TWO") + Environment.NewLine;

        var s3 = new FakeS3 { EncryptedContent = sourceContent };
        var sut = CreateSut(s3);

        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, 2);

        await sut.ExecuteAsync(request, CancellationToken.None);

        s3.PutRequests.Should().BeEmpty();
    }

    [Fact]
    public async Task Uploads_partial_final_chunk()
    {
        var sourceContent = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "C|RECORD_TYPE|COLUMN_ONE|COLUMN_TWO",
            "D|1|One",
            "D|2|Two",
            "D|3|Three") + Environment.NewLine;

        var s3 = new FakeS3 { EncryptedContent = sourceContent };
        var sut = CreateSut(s3);

        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.ByLines, 2);

        await sut.ExecuteAsync(request, CancellationToken.None);

        s3.UploadedContent.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["split-output/source-file-part-0001.csv"] = string.Join(
                Environment.NewLine,
                "record_type|column_one|column_two",
                "D|1|One",
                "D|2|Two",
                string.Empty),
            ["split-output/source-file-part-0002.csv"] = string.Join(
                Environment.NewLine,
                "record_type|column_one|column_two",
                "D|3|Three",
                string.Empty)
        });
    }

    [Fact]
    public async Task Splits_by_size_into_single_chunk_when_small()
    {
        var s3 = new FakeS3 { EncryptedContent = SmallInputFile };
        var sut = CreateSut(s3);

        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.BySize, 1);

        await sut.ExecuteAsync(request, CancellationToken.None);

        s3.UploadedContent.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["split-output/source-file-part-0001.csv"] = SmallInputFile
        });
    }

    [Fact]
    public async Task Splits_by_size_without_prefix_when_prefix_empty()
    {
        var s3 = new FakeS3 { EncryptedContent = SmallInputFile };
        var sut = CreateSut(s3);

        var request = new CsvDataFileSplitJob("", SourceKey, "", SplitType.BySize, 1);

        await sut.ExecuteAsync(request, CancellationToken.None);

        s3.UploadedContent.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["source-file-part-0001.csv"] = SmallInputFile
        });
    }

    [Fact]
    public async Task Splits_large_file_into_multiple_size_chunks()
    {
        var first = new string('A', 600_000);
        var second = new string('B', 600_000);
        var third = new string('C', 100);

        var sourceContent = string.Join(Environment.NewLine, first, second, third, Environment.NewLine);

        var s3 = new FakeS3 { EncryptedContent = sourceContent };
        var sut = CreateSut(s3);

        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.BySize, 1);

        await sut.ExecuteAsync(request, CancellationToken.None);

        s3.UploadedContent.Keys.Should().BeEquivalentTo([
            $"{DestinationPrefix}/source-file-part-0001.csv",
            $"{DestinationPrefix}/source-file-part-0002.csv"
        ]);

        s3.UploadedContent[$"{DestinationPrefix}/source-file-part-0001.csv"]
            .Should().Contain(first)
            .And.NotContain(second);

        s3.UploadedContent[$"{DestinationPrefix}/source-file-part-0002.csv"]
            .Should().Contain(second)
            .And.Contain(third);
    }

    [Fact]
    public async Task Cancels_cleanly()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var s3 = new FakeS3 { EncryptedContent = SmallInputFile };
        var sut = CreateSut(s3);

        var request = new CsvDataFileSplitJob("", SourceKey, DestinationPrefix, SplitType.BySize, 1);

        var result = await sut.ExecuteAsync(request, cts.Token);

        result.Should().BeFalse();
        s3.PutRequests.Should().BeEmpty();
    }

    private static CsvDataFileSplitterService CreateSut(IAmazonS3 s3)
    {
        var factory = new Mock<IS3ClientFactory>();
        factory.Setup(x => x.GetClientInfo<InternalStorageClient>())
            .Returns(new CadsBridge.Infrastructure.Storage.Factories.S3ClientFactory.ClientInfo(s3, BucketName));

        var logger = new Mock<ILogger<CsvDataFileSplitterService>>();
        logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);

        var strategyFactory = new CsvDataFileSplitterFactory(CreateStrategies());

        return new CsvDataFileSplitterService(factory.Object, strategyFactory, logger.Object);
    }

    private static ICsvDataFileSplitterStrategy[] CreateStrategies() =>
    [
        new CsvDataFileSplitterStrategyNone(Mock.Of<ILogger<CsvDataFileSplitterStrategyNone>>()),
        new CsvDataFileSplitterStrategyByLines(Mock.Of<ILogger<CsvDataFileSplitterStrategyByLines>>()),
        new CsvDataFileSplitterStrategyBySize(Mock.Of<ILogger<CsvDataFileSplitterStrategyBySize>>())
    ];
}