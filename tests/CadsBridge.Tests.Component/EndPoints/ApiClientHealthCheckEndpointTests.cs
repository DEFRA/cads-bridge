using System.Net;
using System.Text.Json;
using CadsBridge.Testing.Support.Utilities.Http;
using CadsBridge.Tests.Component.TestFixtures;
using FluentAssertions;

namespace CadsBridge.Tests.Component.EndPoints;

[Collection("ComponentHealthChecks")]
public class ApiClientHealthCheckEndpointTests
{
    private const string ClientName = "downstream-api";
    private static string EntryKey => $"http-client-{ClientName}";

    private static Dictionary<string, string?> EnableApiClient(bool healthcheckEnabled) => new()
    {
        [$"ApiClients:{ClientName}:BaseUrl"] = "http://downstream-api",
        [$"ApiClients:{ClientName}:HealthcheckEnabled"] = healthcheckEnabled.ToString()
        // No ResiliencePolicy overrides needed: the health probe client has no resilience handler.
    };

    [Fact]
    public async Task Health_WhenApiClientHealthy_ReportsHealthyEntryAndOk()
    {
        await using var factory = new CadsBridgeWebAppFactory(EnableApiClient(true));
        factory.OverrideApiClientHealthHandler(ClientName, new StubHttpMessageHandler(HttpStatusCode.OK));
        var client = factory.CreateClient();

        var response = await client.GetAsync("/health", TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        using var doc = JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        var entry = doc.RootElement.GetProperty("results").GetProperty(EntryKey);
        entry.GetProperty("status").GetString().Should().Be("Healthy");
        entry.GetProperty("data").GetProperty("client-name").GetString().Should().Be(ClientName);
    }

    [Fact]
    public async Task Health_WhenApiClientDegraded_OverallStaysOk_ButEntryDegraded()
    {
        await using var factory = new CadsBridgeWebAppFactory(EnableApiClient(true));
        factory.OverrideApiClientHealthHandler(ClientName, new StubHttpMessageHandler(HttpStatusCode.ServiceUnavailable));
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
        factory.OverrideApiClientHealthHandler(ClientName, new StubHttpMessageHandler(new HttpRequestException("refused")));
        var client = factory.CreateClient();

        var response = await client.GetAsync("/health", TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);

        using var doc = JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        var entry = doc.RootElement.GetProperty("results").GetProperty(EntryKey);
        entry.GetProperty("status").GetString().Should().Be("Unhealthy");
        entry.GetProperty("data").TryGetProperty("error", out _).Should().BeTrue();
    }

    [Fact]
    public async Task Health_WhenHealthcheckDisabled_NoApiClientEntry()
    {
        await using var factory = new CadsBridgeWebAppFactory(EnableApiClient(false));
        var client = factory.CreateClient();

        var response = await client.GetAsync("/health", TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        using var doc = JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        doc.RootElement.GetProperty("results").TryGetProperty(EntryKey, out _).Should().BeFalse();
    }
}