using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Endpoints.Requests;
using CadsBridge.Endpoints.Testing.Models;
using CadsBridge.Infrastructure.Json;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.TestFixtures.Containers;
using CadsBridge.Testing.Support.Utilities.Assertions;
using CadsBridge.Testing.Support.Utilities.Http;
using FluentAssertions;
using System.Net;
using System.Text;
using System.Text.Json;

namespace CadsBridge.Tests.Integration.Endpoints;

[Collection("CadsBridgeIntegration"), Trait("Dependence", "testcontainers")]
public class GetInternalFilesEndpointTests(ApiContainerFixture apiContainerFixture)
{
    private const string Route = "/api/v1/test-support/internal-files";

    private const int RecordCount = 11;

    [Fact]
    public async Task GetInternalFiles_AfterUploadAndSplit_ReturnsEveryChunkWithRecordCounts()
    {
        var fileName = NewE2eFileName();
        var stem = Path.GetFileNameWithoutExtension(fileName);
        var chunkFolder = $"import/cts/bulk/{stem}/";

        await UploadAsync(fileName, BuildFile(fileName, RecordCount));
        await PlaceDecoysAsync(stem);
        await TriggerImportAsync($"cads/cts/bulk/{fileName}");

        GetInternalFilesResponse? result = null;
        await AsyncAssert.WaitForAssertion(async () =>
        {
            result = await ListAsync(fileName);
            result.Chunks.Should().HaveCount(3);
        }, backOffMilliSeconds: 500, attempts: 30);

        result!.FileName.Should().Be(fileName);
        result.Chunks.Select(c => c.ChunkNo).Should().Equal(1, 2, 3);
        result.Chunks.Select(c => c.Key).Should().Equal(
            $"{chunkFolder}{stem}-part-0001.csv",
            $"{chunkFolder}{stem}-part-0002.csv",
            $"{chunkFolder}{stem}-part-0003.csv");
        result.Chunks.Select(c => c.RecordCount).Should().Equal(5, 5, 1);
        result.Chunks.Sum(c => c.RecordCount).Should().Be(RecordCount);
        result.Chunks.Should().OnlyContain(c => c.SizeBytes > 0);

        result.Chunks.Should().OnlyContain(c => c.Key.StartsWith(chunkFolder, StringComparison.Ordinal));
        result.Chunks.Should().NotContain(c => c.Key.Contains("decoy", StringComparison.Ordinal));
    }

    [Fact]
    public async Task GetInternalFiles_ForUnknownFile_ReturnsOkWithEmptyChunks()
    {
        var fileName = NewE2eFileName();

        var result = await ListAsync(fileName);

        result.FileName.Should().Be(fileName);
        result.Chunks.Should().BeEmpty();
    }

    [Fact]
    public async Task GetInternalFiles_DoesNotReturnAnotherRunsObjects()
    {
        var fileName = NewE2eFileName();
        var stem = Path.GetFileNameWithoutExtension(fileName);
        await PlaceDecoysAsync(stem);

        var result = await ListAsync(fileName);

        result.Chunks.Should().BeEmpty();
    }

    [Theory]
    [InlineData("not-a-ctsm-file.csv")]
    [InlineData("CTSM_CADS_TEST_FULL_BATCH1_MYTABLE_2026-07-10-120000.csv")]
    [InlineData("CTSM_UKV_PROD_BULK_123456_1_CT_REGISTERED_MOVEMENTS_2026-02-22-074603.csv")]
    [InlineData("import/cts/bulk/CTSM_CADS_ETE_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.csv")]
    public async Task GetInternalFiles_WithBadOrNonE2eFileName_ReturnsBadRequest(string fileName)
    {
        var response = await apiContainerFixture.HttpClient!.GetAsync(
            $"{Route}?fileName={Uri.EscapeDataString(fileName)}", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task GetInternalFiles_WithoutCredentials_ReturnsUnauthorized()
    {
        using var client = new HttpClient { BaseAddress = apiContainerFixture.HttpClient!.BaseAddress };

        var response = await client.GetAsync(
            $"{Route}?fileName={NewE2eFileName()}", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GetInternalFiles_WithFlagOff_ReturnsNotFound()
    {
        await using var fixture = new ApiContainerWithEnvsFixture(new Dictionary<string, string>
        {
            ["EnableTestSupportEndpoints"] = "false"
        });
        await fixture.InitializeAsync();

        var response = await fixture.HttpClient!.GetAsync(
            $"{Route}?fileName={NewE2eFileName()}", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    private static string NewE2eFileName() =>
        $"CTSM_CADS_ETE_BULK_{Random.Shared.Next(1, 99999):D5}_001_CT_LOCATIONS_{DateTime.UtcNow:yyyy-MM-dd-HHmmss}.csv";

    private static string BuildFile(string fileName, int records)
    {
        var builder = new StringBuilder();
        builder.Append("HEADER|ignored\n");
        builder.Append("C|RECORD_TYPE|COLUMN_ONE|COLUMN_TWO\n");
        for (var i = 1; i <= records; i++)
        {
            builder.Append($"D|{i}|Value {i}\n");
        }

        builder.Append($"T|{fileName}|01012000 00:00:00|{records}\n");
        return builder.ToString();
    }

    private async Task UploadAsync(string fileName, string content)
    {
        var response = await apiContainerFixture.HttpClient!.PostAsync(
            "/api/v1/test-support/files",
            HttpContentUtility.CreateApplicationJsonAsStringContent(
                new CreateTestFileRequest(fileName, content, DataSourceType.CtsBulk)),
            TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    private async Task TriggerImportAsync(string externalKey)
    {
        var response = await apiContainerFixture.HttpClient!.PostAsync(
            "import",
            HttpContentUtility.CreateApplicationJsonAsStringContent(
                new CsvDataFileImportRequest([new CsvDataFileImportRequestItem(externalKey)])),
            TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    private async Task<GetInternalFilesResponse> ListAsync(string fileName)
    {
        var response = await apiContainerFixture.HttpClient!.GetAsync(
            $"{Route}?fileName={Uri.EscapeDataString(fileName)}", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        return JsonSerializer.Deserialize<GetInternalFilesResponse>(body, JsonDefaults.DefaultOptions)!;
    }

    private async Task PlaceDecoysAsync(string stem)
    {
        var otherRun = stem.Replace("_ETE_BULK_", "_ETE_BULK_X", StringComparison.Ordinal);
        var nonE2e = stem.Replace("CTSM_CADS_ETE_", "CTSM_UKV_PROD_", StringComparison.Ordinal);

        string[] keys =
        [
            $"import/cts/bulk/{otherRun}/{otherRun}-part-0001.csv",
            $"import/cts/bulk/{nonE2e}/{nonE2e}-part-0001.csv",
            $"import/cts/bulk/{stem}decoy/{stem}decoy-part-0001.csv",
            $"import/cts/daily/{stem}/{stem}-part-0001.csv",
            $"import/cts/bulk/{stem}/decoy.csv"
        ];

        foreach (var key in keys)
        {
            await apiContainerFixture.LocalStackFixture.S3Client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = TestS3Constants.TestCadsBridgeInternalBucketName,
                Key = key,
                ContentBody = "record_type|decoy\nD|1|decoy\n"
            }, TestContext.Current.CancellationToken);
        }
    }
}
