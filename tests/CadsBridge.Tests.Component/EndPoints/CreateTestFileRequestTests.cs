using System.Net;
using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Endpoints.Testing.Models;
using CadsBridge.Testing.Support.Utilities.Http;
using CadsBridge.Tests.Component.TestFixtures;
using FluentAssertions;

namespace CadsBridge.Tests.Component.EndPoints;

public class CreateTestFileRequestTests
{
    [Fact]
    public async Task ICreateTestFileRequest_WithValidRequest_ReturnsOk()
    {
        var configOverrides = new Dictionary<string, string?>
        {
            ["EnableTestEndpoints"] = "true"
        };
        await using var factory = new CadsBridgeWebAppFactory(configOverrides);
        var response = await TriggerImportJob(factory);
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ICreateTestFileRequest_WithNoFileName_ReturnsBadRequest()
    {
        var configOverrides = new Dictionary<string, string?>
        {
            ["EnableTestEndpoints"] = "true"
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
            ["EnableTestEndpoints"] = "true"
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
            ["EnableTestEndpoints"] = "true"
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