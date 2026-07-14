using CadsBridge.Infrastructure.ApiClients.Configuration;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Testing.Support.Utilities.Http;
using CadsBridge.Tests.Component.TestFixtures;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using System.Net;

namespace CadsBridge.Tests.Component.ApiClients;

public class FileImportStatusApiServiceComponentTests
{
    private const string ClientName = nameof(ApiClientNames.CdsApi);

    // A valid absolute BaseUrl is required or AddApiClients skips registering the client entirely.
    private static Dictionary<string, string?> CdsApiConfig() => new()
    {
        [$"ApiClients:{ClientName}:BaseUrl"] = "http://cds-api",
        [$"ApiClients:{ClientName}:HealthcheckEnabled"] = "false",
        [$"ApiClients:{ClientName}:ResiliencePolicy:Retries"] = "1",
        [$"ApiClients:{ClientName}:ResiliencePolicy:BaseDelaySeconds"] = "0",
        [$"ApiClients:{ClientName}:ResiliencePolicy:UseJitter"] = "false"
    };

    [Theory]
    [InlineData(FileImportStatus.Importing, "importing")]
    [InlineData(FileImportStatus.Completed, "complete")]
    [InlineData(FileImportStatus.Failed, "failed")]
    public async Task MarkStatus_SendsPostThroughRealCdsApiClient_ToExpectedUrl(
        FileImportStatus status, string segment)
    {
        await using var factory = new CadsBridgeWebAppFactory(CdsApiConfig());
        var stub = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK));
        factory.OverrideHttpClientHandler(ClientName, stub);
        _ = factory.CreateClient(); // force host build so ConfigureTestServices runs

        var sut = factory.Services.GetRequiredService<IFileImportStatusApiService>();
        await sut.MarkStatus(99, status, TestContext.Current.CancellationToken);

        stub.Requests.Should().ContainSingle();
        stub.Requests[0].Method.Should().Be(HttpMethod.Post);
        stub.Requests[0].RequestUri!.AbsolutePath
            .Should().Be($"/api/v1/systemadmin/fileimports/99/{segment}");
    }

    [Fact]
    public async Task MarkReset_SendsPostThroughRealCdsApiClient_ToExpectedUrl()
    {
        await using var factory = new CadsBridgeWebAppFactory(CdsApiConfig());
        var stub = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK));
        factory.OverrideHttpClientHandler(ClientName, stub);
        _ = factory.CreateClient();

        var sut = factory.Services.GetRequiredService<IFileImportStatusApiService>();
        await sut.MarkReset(99, TestContext.Current.CancellationToken);

        stub.Requests.Should().ContainSingle();
        stub.Requests[0].Method.Should().Be(HttpMethod.Post);
        stub.Requests[0].RequestUri!.AbsolutePath
            .Should().Be("/api/v1/systemadmin/fileimports/99/reset");
    }
}