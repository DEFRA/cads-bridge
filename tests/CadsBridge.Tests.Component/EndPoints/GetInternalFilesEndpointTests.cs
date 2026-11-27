using CadsBridge.Application.DataLoad.Services.Testing;
using CadsBridge.Testing.Support.Utilities.Http;
using CadsBridge.Tests.Component.TestFixtures;
using FluentAssertions;
using Moq;
using System.Net;
using System.Text.Json;

namespace CadsBridge.Tests.Component.Endpoints;

public class GetInternalFilesEndpointTests
{
    private const string Route = "/api/v1/test-support/internal-files";
    private const string FileName = "CTSM_CADS_ETE_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.csv";

    private static readonly Dictionary<string, string?> FlagOn = new() { ["EnableTestSupportEndpoints"] = "true" };

    [Fact]
    public async Task GetInternalFiles_ForUnknownFile_ReturnsOkWithEmptyChunks()
    {
        await using var factory = new CadsBridgeWebAppFactory(FlagOn);
        var client = factory.CreateClient().AddBasicTestApiKey();

        var response = await client.GetAsync($"{Route}?fileName={FileName}", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        body.RootElement.GetProperty("fileName").GetString().Should().Be(FileName);
        body.RootElement.GetProperty("chunks").GetArrayLength().Should().Be(0);
    }

    [Fact]
    public async Task GetInternalFiles_ReturnsChunksInCamelCaseContract()
    {
        var inspection = new Mock<IInternalFileService>();
        inspection.Setup(x => x.ListChunksAsync(FileName, It.IsAny<CancellationToken>()))
            .ReturnsAsync([
                new InternalFileChunk("import/cts/bulk/x/x-part-0001.csv", 1, 123, 5),
                new InternalFileChunk("import/cts/bulk/x/x-part-0002.csv", 2, 45, 1)
            ]);

        await using var factory = new CadsBridgeWebAppFactory(FlagOn);
        factory.OverrideSingleton(inspection.Object);
        var client = factory.CreateClient().AddBasicTestApiKey();

        var response = await client.GetAsync($"{Route}?fileName={FileName}", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        var chunks = body.RootElement.GetProperty("chunks");
        chunks.GetArrayLength().Should().Be(2);
        var first = chunks[0];
        first.GetProperty("key").GetString().Should().Be("import/cts/bulk/x/x-part-0001.csv");
        first.GetProperty("chunkNo").GetInt32().Should().Be(1);
        first.GetProperty("sizeBytes").GetInt64().Should().Be(123);
        first.GetProperty("recordCount").GetInt64().Should().Be(5);
    }

    [Theory]
    [InlineData("")]
    [InlineData("?fileName=")]
    [InlineData("?fileName=not-a-ctsm-file.csv")]
    public async Task GetInternalFiles_WithBadOrNonE2eFileName_ReturnsBadRequest(string query)
    {
        await using var factory = new CadsBridgeWebAppFactory(FlagOn);
        var client = factory.CreateClient().AddBasicTestApiKey();

        var response = await client.GetAsync($"{Route}{query}", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task GetInternalFiles_WithoutCredentials_ReturnsUnauthorized()
    {
        await using var factory = new CadsBridgeWebAppFactory(FlagOn);
        var client = factory.CreateClient();

        var response = await client.GetAsync($"{Route}?fileName={FileName}", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GetInternalFiles_WithFlagOff_ReturnsNotFound()
    {
        await using var factory = new CadsBridgeWebAppFactory(new Dictionary<string, string?>
        {
            ["EnableTestSupportEndpoints"] = "false"
        });
        var client = factory.CreateClient().AddBasicTestApiKey();

        var response = await client.GetAsync($"{Route}?fileName={FileName}", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Theory]
    [InlineData("POST")]
    [InlineData("PUT")]
    [InlineData("DELETE")]
    [InlineData("PATCH")]
    public async Task GetInternalFiles_IsReadOnly(string method)
    {
        await using var factory = new CadsBridgeWebAppFactory(FlagOn);
        var client = factory.CreateClient().AddBasicTestApiKey();

        var response = await client.SendAsync(
            new HttpRequestMessage(new HttpMethod(method), $"{Route}?fileName={FileName}"),
            TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.MethodNotAllowed);
    }
}
