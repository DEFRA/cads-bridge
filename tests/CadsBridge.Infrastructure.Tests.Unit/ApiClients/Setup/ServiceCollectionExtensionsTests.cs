using CadsBridge.Infrastructure.ApiClients.Configuration;
using CadsBridge.Infrastructure.ApiClients.Setup;
using CadsBridge.Testing.Support.Utilities.Http;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using System.Net;
using ApiClientSetup = CadsBridge.Infrastructure.ApiClients.Setup.ServiceCollectionExtensions;

namespace CadsBridge.Infrastructure.Tests.Unit.ApiClients.Setup;

public class ServiceCollectionExtensionsTests
{
    private const string ClientName = nameof(ApiClientNames.CdsApi);

    private static IConfiguration BuildConfig(Dictionary<string, string?> values) =>
        new ConfigurationBuilder().AddInMemoryCollection(values).Build();

    private static (ServiceCollection Services, IHealthChecksBuilder HealthChecks) CreateServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var healthChecks = services.AddHealthChecks();
        return (services, healthChecks);
    }

    private static List<HealthCheckRegistration> GetRegistrations(IServiceProvider sp) =>
        [.. sp.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations];

    [Fact]
    public void HealthClientName_AppendsHealthSuffix()
    {
        ApiClientSetup.HealthClientName("foo").Should().Be("foo-health");
    }

    [Fact]
    public void AddApiClients_WhenNoApiClientsSection_RegistersNothing()
    {
        var (services, healthChecks) = CreateServices();
        var config = BuildConfig(new Dictionary<string, string?>());

        services.AddApiClients(config, healthChecks);

        var sp = services.BuildServiceProvider();
        GetRegistrations(sp).Should().BeEmpty();
    }

    [Fact]
    public void AddApiClients_WhenHealthcheckEnabled_RegistersHealthCheckAndBothClients()
    {
        var (services, healthChecks) = CreateServices();
        var config = BuildConfig(new Dictionary<string, string?>
        {
            [$"ApiClients:{ClientName}:BaseUrl"] = "http://test-api/",
            [$"ApiClients:{ClientName}:HealthcheckEnabled"] = "true"
        });

        services.AddApiClients(config, healthChecks);

        var sp = services.BuildServiceProvider();
        GetRegistrations(sp).Should().ContainSingle(r => r.Name == $"http-client-{ClientName}");

        var factory = sp.GetRequiredService<IHttpClientFactory>();
        factory.CreateClient(ClientName).BaseAddress.Should().Be(new Uri("http://test-api"));
        factory.CreateClient(ApiClientSetup.HealthClientName(ClientName))
            .BaseAddress.Should().Be(new Uri("http://test-api"));
    }

    [Fact]
    public void AddApiClients_WhenHealthcheckDisabled_RegistersClientButNoHealthCheck()
    {
        var (services, healthChecks) = CreateServices();
        var config = BuildConfig(new Dictionary<string, string?>
        {
            [$"ApiClients:{ClientName}:BaseUrl"] = "http://test-api",
            [$"ApiClients:{ClientName}:HealthcheckEnabled"] = "false"
        });

        services.AddApiClients(config, healthChecks);

        var sp = services.BuildServiceProvider();
        GetRegistrations(sp).Should().BeEmpty();
        sp.GetRequiredService<IHttpClientFactory>().CreateClient(ClientName)
            .BaseAddress.Should().Be(new Uri("http://test-api"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("Set in cdp-app-config (or for local development using docker-compose.override.yml)")]
    [InlineData("not-a-uri")]
    public void AddApiClients_WhenBaseUrlIsNotAValidAbsoluteUri_SkipsClientAndHealthCheck(string baseUrl)
    {
        var (services, healthChecks) = CreateServices();
        var config = BuildConfig(new Dictionary<string, string?>
        {
            [$"ApiClients:{ClientName}:BaseUrl"] = baseUrl,
            [$"ApiClients:{ClientName}:HealthcheckEnabled"] = "true"
        });

        services.AddApiClients(config, healthChecks);

        var sp = services.BuildServiceProvider();
        GetRegistrations(sp).Should().BeEmpty("a placeholder/invalid BaseUrl should be skipped, not fail the health endpoint");
    }

    [Theory]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.RequestTimeout)]
    public async Task ResilienceHandler_RetriesOnTransientStatus(HttpStatusCode statusCode)
    {
        var stub = new StubHttpMessageHandler(statusCode);
        var client = BuildResilientClient(stub, retries: 2);

        var response = await client.GetAsync("/health", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(statusCode);
        stub.Requests.Should().HaveCount(3); // 1 initial attempt + 2 retries
    }

    [Fact]
    public async Task ResilienceHandler_RetriesOnHttpRequestException()
    {
        var stub = new StubHttpMessageHandler(new HttpRequestException("boom"));
        var client = BuildResilientClient(stub, retries: 2);

        var act = async () => await client.GetAsync("/health", TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<HttpRequestException>();
        stub.Requests.Should().HaveCount(3);
    }

    [Theory]
    [InlineData(HttpStatusCode.OK)]
    [InlineData(HttpStatusCode.BadRequest)]
    [InlineData(HttpStatusCode.NotFound)]
    public async Task ResilienceHandler_DoesNotRetryOnNonTransientResponse(HttpStatusCode statusCode)
    {
        var stub = new StubHttpMessageHandler(statusCode);
        var client = BuildResilientClient(stub, retries: 2);

        var response = await client.GetAsync("/health", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(statusCode);
        stub.Requests.Should().ContainSingle(); // no retries
    }

    private static HttpClient BuildResilientClient(HttpMessageHandler primaryHandler, int retries)
    {
        var (services, healthChecks) = CreateServices();
        var config = BuildConfig(new Dictionary<string, string?>
        {
            [$"ApiClients:{ClientName}:BaseUrl"] = "http://test-api",
            [$"ApiClients:{ClientName}:HealthcheckEnabled"] = "false",
            [$"ApiClients:{ClientName}:ResiliencePolicy:Retries"] = retries.ToString(),
            [$"ApiClients:{ClientName}:ResiliencePolicy:BaseDelaySeconds"] = "0",
            [$"ApiClients:{ClientName}:ResiliencePolicy:UseJitter"] = "false",
            [$"ApiClients:{ClientName}:ResiliencePolicy:TimeoutPeriodSeconds"] = "30"
        });

        services.AddApiClients(config, healthChecks);
        services.AddHttpClient(ClientName).ConfigurePrimaryHttpMessageHandler(() => primaryHandler);

        var sp = services.BuildServiceProvider();
        return sp.GetRequiredService<IHttpClientFactory>().CreateClient(ClientName);
    }
}