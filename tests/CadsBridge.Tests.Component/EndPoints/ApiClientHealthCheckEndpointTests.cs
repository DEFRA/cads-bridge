using CadsBridge.Infrastructure.ApiClients.Configuration;
using CadsBridge.Testing.Support.Utilities.Http;
using CadsBridge.Tests.Component.TestFixtures;
using FluentAssertions;
using System.Net;
using System.Text.Json;

namespace CadsBridge.Tests.Component.Endpoints;

[Collection("ComponentHealthChecks")]
public class ApiClientHealthCheckEndpointTests
{
    private const string ClientName = nameof(ApiClientNames.CdsApi);
    private static string EntryKey => $"http-client-{ClientName}";

    private static Dictionary<string, string?> EnableApiClient(bool healthcheckEnabled) => new()
    {
        [$"ApiClients:{ClientName}:BaseUrl"] = "http://downstream-api",
        [$"ApiClients:{ClientName}:HealthcheckEnabled"] = healthcheckEnabled.ToString()
    };

    [Fact]
    public async Task Health_WhenApiClientHealthy_ReportsHealthyEntryAndOk()
    {
        await using var factory = new CadsBridgeWebAppFactory(EnableApiClient(true));
        factory.OverrideHttpClientHandler(ClientName, new StubHttpMessageHandler(HttpStatusCode.OK));
        var client = factory.CreateClient();

        var response = await client.GetAsync("/health", TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        using var doc = JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        var entry = doc.RootElement.GetProperty("results").GetProperty(EntryKey);
        entry.GetProperty("status").GetString().Should().Be("Healthy");
        entry.GetProperty("data").GetProperty("client-name").GetString().Should().Be(ClientName);
    }

#pragma warning disable xUnit1004 // Test methods should not be skipped
    [Fact(Skip = "To investigate")]
#pragma warning restore xUnit1004 // Test methods should not be skipped
    public async Task Health_WhenApiClientDegraded_OverallStaysOk_ButEntryDegraded()
    {
        await using var factory = new CadsBridgeWebAppFactory(EnableApiClient(true));
        factory.OverrideHttpClientHandler(ClientName, new StubHttpMessageHandler(HttpStatusCode.ServiceUnavailable));
        var client = factory.CreateClient();

        var response = await client.GetAsync("/health", TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.OK); // Degraded -> 200

        using var doc = JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        doc.RootElement.GetProperty("status").GetString().Should().Be("Degraded");
        doc.RootElement.GetProperty("results").GetProperty(EntryKey)
            .GetProperty("status").GetString().Should().Be("Degraded");
    }

    [Fact]
    public async Task Health_WhenApiClientUnreachable_OverallUnhealthyWithUnhealthyEntry()
    {
        await using var factory = new CadsBridgeWebAppFactory(EnableApiClient(true));
        factory.OverrideHttpClientHandler(ClientName, new StubHttpMessageHandler(new HttpRequestException("refused")));
        var client = factory.CreateClient();

        var response = await client.GetAsync("/health", TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);

        using var doc = JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        var entry = doc.RootElement.GetProperty("results").GetProperty(EntryKey);
        entry.GetProperty("status").GetString().Should().Be("Unhealthy");
        entry.GetProperty("data").TryGetProperty("error", out _).Should().BeTrue();
    }
}