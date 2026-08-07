using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using CadsBridge.Infrastructure.DataLoad.Csv.Factories;
using CadsBridge.Infrastructure.DataLoad.Csv.Services;
using CadsBridge.Infrastructure.DataLoad.Csv.Strategies;
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

    private static string SmallInputFile => string.Join(
        Environment.NewLine,
        "HEADER|ignored",
        "C|RECORD_TYPE|COLUMN_ONE|COLUMN_TWO",
        "D|1|One") + Environment.NewLine;

    private static string ExpectedKey(int part = 1) => $"import/source-file/source-file-part-{part:D4}.csv";

    [Fact]
    public async Task Throws_when_split_value_is_null_for_by_lines()
    {
        var sut = CreateSut(new FakeS3(), SplitType.ByLines, null);
        var request = new CsvDataFileSplitJob(SourceKey, null);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            sut.ExecuteAsync(request, CancellationToken.None));
    }

    [Fact]
    public async Task Throws_when_split_value_is_null_for_by_size()
    {
        var sut = CreateSut(new FakeS3(), SplitType.BySize, null);
        var request = new CsvDataFileSplitJob(SourceKey, null);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            sut.ExecuteAsync(request, CancellationToken.None));
    }

    [Fact]
    public async Task Throws_when_split_type_is_invalid()
    {
        var sut = CreateSut(new FakeS3(), (SplitType)999, 2);
        var request = new CsvDataFileSplitJob(SourceKey, null);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            sut.ExecuteAsync(request, CancellationToken.None));
    }

    [Fact]
    public async Task Copies_whole_file_as_single_part_when_split_type_none()
    {
        var s3 = new FakeS3 { EncryptedContent = SmallInputFile };
        var sut = CreateSut(s3, SplitType.None, null);

        var request = new CsvDataFileSplitJob(SourceKey, null);

        var result = await sut.ExecuteAsync(request, CancellationToken.None);

        result.Should().Be(SmallInputFile.Length);
        s3.UploadedContent.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            [ExpectedKey()] = SmallInputFile
        });
    }

    [Fact]
    public async Task Retries_when_file_fails_to_load()
    {
        var s3 = new Mock<IAmazonS3>();

        s3.Setup(x => x.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
          .Throws<NullReferenceException>();

        var sut = CreateSut(s3.Object, SplitType.ByLines, 2);

        var request = new CsvDataFileSplitJob(SourceKey, null);

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
            "D|Dana|White",
            "T|ignored") + Environment.NewLine;

        var s3 = new FakeS3 { EncryptedContent = sourceContent };
        var sut = CreateSut(s3, SplitType.ByLines, 2);

        var request = new CsvDataFileSplitJob(SourceKey, null);

        var result = await sut.ExecuteAsync(request, CancellationToken.None);

        result.Should().Be(4);

        s3.UploadedContent.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            [ExpectedKey(1)] = string.Join(
                Environment.NewLine,
                "record_type|first_name|last_name",
                "D|Alice|Smith",
                "D|Bob|Jones",
                string.Empty),
            [ExpectedKey(2)] = string.Join(
                Environment.NewLine,
                "record_type|first_name|last_name",
                "D|Charlie|Brown",
                "D|Dana|White",
                string.Empty),
            [ExpectedKey(3)] = string.Join(
                Environment.NewLine,
                "record_type|first_name|last_name",
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
        var sut = CreateSut(s3, SplitType.ByLines, 2);

        var request = new CsvDataFileSplitJob(SourceKey, null);

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
            "D|3|Three",
            "T|ignored") + Environment.NewLine;

        var s3 = new FakeS3 { EncryptedContent = sourceContent };
        var sut = CreateSut(s3, SplitType.ByLines, 2);

        var request = new CsvDataFileSplitJob(SourceKey, null);

        var result = await sut.ExecuteAsync(request, CancellationToken.None);

        result.Should().Be(3);

        s3.UploadedContent.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            [ExpectedKey(1)] = string.Join(
                Environment.NewLine,
                "record_type|column_one|column_two",
                "D|1|One",
                "D|2|Two",
                string.Empty),
            [ExpectedKey(2)] = string.Join(
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
        var sut = CreateSut(s3, SplitType.BySize, 1);

        var request = new CsvDataFileSplitJob(SourceKey, null);

        await sut.ExecuteAsync(request, CancellationToken.None);

        s3.UploadedContent.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            [ExpectedKey()] = SmallInputFile
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
        var sut = CreateSut(s3, SplitType.BySize, 1);

        var request = new CsvDataFileSplitJob(SourceKey, null);

        await sut.ExecuteAsync(request, CancellationToken.None);

        s3.UploadedContent.Keys.Should().BeEquivalentTo([ExpectedKey(1), ExpectedKey(2)]);

        s3.UploadedContent[ExpectedKey(1)]
            .Should().Contain(first)
            .And.NotContain(second);

        s3.UploadedContent[ExpectedKey(2)]
            .Should().Contain(second)
            .And.Contain(third);
    }

    [Fact]
    public async Task Cancels_cleanly()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var s3 = new FakeS3 { EncryptedContent = SmallInputFile };
        var sut = CreateSut(s3, SplitType.BySize, 1);

        var request = new CsvDataFileSplitJob(SourceKey, null);

        var result = await sut.ExecuteAsync(request, cts.Token);

        result.Should().Be(0);
        s3.PutRequests.Should().BeEmpty();
    }

    private static CsvDataFileSplitterService CreateSut(IAmazonS3 s3, SplitType splitType, int? splitValue)
    {
        var factory = new Mock<IS3ClientFactory>();
        factory.Setup(x => x.GetClientInfo<InternalStorageClient>())
            .Returns(new CadsBridge.Infrastructure.Storage.Factories.S3ClientFactory.ClientInfo(s3, BucketName));

        var config = new DataLoadConfiguration { SplitType = splitType, SplitValue = splitValue };

        var logger = new Mock<ILogger<CsvDataFileSplitterService>>();
        logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);

        var strategyFactory = new CsvDataFileSplitterFactory(CreateStrategies(factory.Object, config));

        return new CsvDataFileSplitterService(strategyFactory, config, logger.Object);
    }

    private static ICsvDataFileSplitterStrategy[] CreateStrategies(IS3ClientFactory factory, DataLoadConfiguration config) =>
    [
        new CsvDataFileSplitterStrategyNone(factory, Mock.Of<ILogger<CsvDataFileSplitterStrategyNone>>()),
        new CsvDataFileSplitterStrategyByLines(factory, config, Mock.Of<ILogger<CsvDataFileSplitterStrategyByLines>>()),
        new CsvDataFileSplitterStrategyBySize(factory, config, Mock.Of<ILogger<CsvDataFileSplitterStrategyBySize>>())
    ];
}