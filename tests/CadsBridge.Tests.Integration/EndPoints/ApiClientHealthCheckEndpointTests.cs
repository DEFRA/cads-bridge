using CadsBridge.Testing.Support.TestFixtures.Containers;
using FluentAssertions;
using System.Net;
using System.Text.Json;

namespace CadsBridge.Tests.Integration.EndPoints;

[Collection("CadsBridgeIntegration"), Trait("Dependence", "testcontainers")]
public class ApiClientHealthCheckEndpointTests
{
    private const string ClientName = "localstack-probe";
    private static string EntryKey => $"http-client-{ClientName}";

    [Fact]
    public async Task Health_WithApiClientHealthcheckEnabled_RegistersClientEntry()
    {
        // Point the API client at LocalStack (already on the shared docker network) so the
        // dedicated health probe reaches a reachable endpoint and the full wiring is exercised:
        // env config -> AddApiClients -> health-check registration -> /health JSON output.
        await using var fixture = new ApiContainerWithEnvsFixture(new Dictionary<string, string>
        {
            [$"ApiClients__{ClientName}__BaseUrl"] = LocalStackFixture.NetworkServiceUrl,
            [$"ApiClients__{ClientName}__HealthcheckEnabled"] = "true"
        });
        await fixture.InitializeAsync();

        var response = await fixture.HttpClient!.GetAsync("/health", TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        using var doc = JsonDocument.Parse(body);
        doc.RootElement.GetProperty("results")
            .TryGetProperty(EntryKey, out var entry)
            .Should().BeTrue("the API client health check should be registered and reported");
        entry.GetProperty("data").GetProperty("client-name").GetString().Should().Be(ClientName);
        entry.GetProperty("status").GetString().Should().BeOneOf("Healthy", "Degraded");
    }

    [Fact]
    public async Task Health_WithApiClientHealthcheckDisabled_DoesNotRegisterClientEntry()
    {
        await using var fixture = new ApiContainerWithEnvsFixture(new Dictionary<string, string>
        {
            [$"ApiClients__{ClientName}__BaseUrl"] = LocalStackFixture.NetworkServiceUrl,
            [$"ApiClients__{ClientName}__HealthcheckEnabled"] = "false"
        });
        await fixture.InitializeAsync();

        var response = await fixture.HttpClient!.GetAsync("/health", TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        using var doc = JsonDocument.Parse(body);
        doc.RootElement.GetProperty("results").TryGetProperty(EntryKey, out _).Should().BeFalse();
    }
}