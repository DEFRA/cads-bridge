using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Endpoints.Testing.Models;
using CadsBridge.Testing.Support.Utilities.Http;
using CadsBridge.Tests.Component.TestFixtures;
using FluentAssertions;
using System.Net;

namespace CadsBridge.Tests.Component.Endpoints;

public class CreateTestFileRequestTests
{
    [Fact]
    public async Task ICreateTestFileRequest_WithValidRequest_ReturnsOk()
    {
        var configOverrides = new Dictionary<string, string?>
        {
            ["EnableTestSupportEndpoints"] = "true"
        };

        var request = new CreateTestFileRequest(
            "CTSM_CADS_TEST_FULL_BATCH1_MYTABLE_2026-07-10-120000.csv",
            "some content",
            DataSourceType.CtsBulk
        );
        await using var factory = new CadsBridgeWebAppFactory(configOverrides);
        var response = await TriggerImportJob(factory, request);
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task ICreateTestFileRequest_WithNoFileName_ReturnsBadRequest()
    {
        var configOverrides = new Dictionary<string, string?>
        {
            ["EnableTestSupportEndpoints"] = "true"
        };
        await using var factory = new CadsBridgeWebAppFactory(configOverrides);
        var response = await TriggerImportJob(factory, new CreateTestFileRequest(
            string.Empty,
            "some content",
            DataSourceType.CtsBulk
        ));
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task ICreateTestFileRequest_WithNoContent_ReturnsBadRequest()
    {
        var configOverrides = new Dictionary<string, string?>
        {
            ["EnableTestSupportEndpoints"] = "true"
        };
        await using var factory = new CadsBridgeWebAppFactory(configOverrides);

        var response = await TriggerImportJob(factory, new CreateTestFileRequest(
            "some-file.csv",
            string.Empty,
            DataSourceType.CtsBulk
        ));

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task ICreateTestFileRequest_WithTooLargeContent_ReturnsBadRequest()
    {
        var configOverrides = new Dictionary<string, string?>
        {
            ["EnableTestSupportEndpoints"] = "true"
        };
        var characterCount = 64 * 1024 + 1;
        var largeString = new string('A', characterCount);
        await using var factory = new CadsBridgeWebAppFactory(configOverrides);

        var response = await TriggerImportJob(factory, new CreateTestFileRequest(
            "some-file.csv",
            largeString,
            DataSourceType.CtsBulk
        ));

        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    // Contract: only camelCase fileName binds; other casings must not be guessed.
    [Theory]
    [InlineData("fileName", HttpStatusCode.Created)]
    [InlineData("filename", HttpStatusCode.BadRequest)]
    [InlineData("FileName", HttpStatusCode.BadRequest)]
    public async Task ICreateTestFileRequest_FileNameCasing_OnlyCamelCaseIsAccepted(string propertyName,
        HttpStatusCode expected)
    {
        var configOverrides = new Dictionary<string, string?>
        {
            ["EnableTestSupportEndpoints"] = "true"
        };
        await using var factory = new CadsBridgeWebAppFactory(configOverrides);
        var json = $$"""
            {"{{propertyName}}":"CTSM_CADS_TEST_FULL_BATCH1_MYTABLE_2026-07-10-120000.csv","content":"some content","dataSource":{{(int)DataSourceType.CtsBulk}}}
            """;
        var client = factory.CreateClient().AddBasicTestApiKey();

        var response = await client.PostAsync("/api/v1/test-support/files",
            HttpContentUtility.CreateApplicationJsonAsStringContent(json), TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(expected);
    }

    // Contract (snapshot) of the upload response property names consumed by the e2e journey tests.
    [Fact]
    public async Task ICreateTestFileRequest_WithValidRequest_ResponseUsesExactCamelCasePropertyNames()
    {
        var configOverrides = new Dictionary<string, string?>
        {
            ["EnableTestSupportEndpoints"] = "true"
        };
        await using var factory = new CadsBridgeWebAppFactory(configOverrides);

        var response = await TriggerImportJob(factory, new CreateTestFileRequest(
            "CTSM_CADS_TEST_FULL_BATCH1_MYTABLE_2026-07-10-120000.csv",
            "some content",
            DataSourceType.CtsBulk));

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        using var document = System.Text.Json.JsonDocument.Parse(body);
        document.RootElement.EnumerateObject().Select(p => p.Name).Should()
            .Equal("fileName", "externalS3Bucket", "externalS3Key", "sizeBytes", "uploadedAt");
    }

    private static async Task<HttpResponseMessage> TriggerImportJob(CadsBridgeWebAppFactory factory,
        CreateTestFileRequest? request = null)
    {
        var content = HttpContentUtility.CreateApplicationJsonAsStringContent(request ??
                                                                              new CreateTestFileRequest("some-file.csv",
                                                                                  "some content",
                                                                                  DataSourceType.CtsBulk));
        var client = factory.CreateClient().AddBasicTestApiKey();
        var response =
            await client.PostAsync("/api/v1/test-support/files", content, TestContext.Current.CancellationToken);

        return response;
    }
}