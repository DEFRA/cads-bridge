using CadsBridge.Infrastructure.ApiClients.Configuration;
using CadsBridge.Infrastructure.ApiClients.Setup;
using CadsBridge.Infrastructure.Authentication.DTOs;
using CadsBridge.Infrastructure.Authentication.Providers;
using CadsBridge.Testing.Support.Utilities.Http;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Moq;
using System.Net;

namespace CadsBridge.Infrastructure.Tests.Unit.ApiClients.Setup;

public class ServiceCollectionExtensionsTests
{
    private const string ClientName = nameof(ApiClientNames.CdsApi);

    private static IConfiguration BuildConfig(Dictionary<string, string?> values) =>
        new ConfigurationBuilder().AddInMemoryCollection(values).Build();

    private static ServiceCollection CreateServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        return services;
    }

    private static List<HealthCheckRegistration> GetRegistrations(IServiceProvider sp) =>
        [.. sp.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations];

    [Fact]
    public void AddApiClients_WhenNoApiClientsSection_RegistersNothing()
    {
        var services = CreateServices();
        var config = BuildConfig([]);

        services.AddApiClients(config);

        var sp = services.BuildServiceProvider();
        GetRegistrations(sp).Should().BeEmpty();
    }

    [Fact]
    public void AddApiClients_WhenHealthcheckEnabled_RegistersHealthCheckAndBothClients()
    {
        var services = CreateServices();
        var config = BuildConfig(new Dictionary<string, string?>
        {
            [$"ApiClients:{ClientName}:BaseUrl"] = "http://test-api/",
            [$"ApiClients:{ClientName}:HealthcheckEnabled"] = "true"
        });

        services.AddApiClients(config);

        var sp = services.BuildServiceProvider();
        GetRegistrations(sp).Should().ContainSingle(r => r.Name == $"http-client-{ClientName}");

        var factory = sp.GetRequiredService<IHttpClientFactory>();
        factory.CreateClient(ClientName).BaseAddress.Should().Be(new Uri("http://test-api"));
    }

    [Fact]
    public void AddApiClients_WhenHealthcheckDisabled_RegistersClientButNoHealthCheck()
    {
        var services = CreateServices();
        var config = BuildConfig(new Dictionary<string, string?>
        {
            [$"ApiClients:{ClientName}:BaseUrl"] = "http://test-api",
            [$"ApiClients:{ClientName}:HealthcheckEnabled"] = "false"
        });

        services.AddApiClients(config);

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
        var services = CreateServices();
        var config = BuildConfig(new Dictionary<string, string?>
        {
            [$"ApiClients:{ClientName}:BaseUrl"] = baseUrl,
            [$"ApiClients:{ClientName}:HealthcheckEnabled"] = "true"
        });

        services.AddApiClients(config);

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

        var response = await client.GetAsync("/health/details", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(statusCode);
        stub.Requests.Should().HaveCount(3); // 1 initial attempt + 2 retries
    }

    [Fact]
    public async Task ResilienceHandler_RetriesOnHttpRequestException()
    {
        var stub = new StubHttpMessageHandler(new HttpRequestException("boom"));
        var client = BuildResilientClient(stub, retries: 2);

        var act = async () => await client.GetAsync("/health/details", TestContext.Current.CancellationToken);

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

        var response = await client.GetAsync("/health/details", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(statusCode);
        stub.Requests.Should().ContainSingle(); // no retries
    }

    [Fact]
    public async Task AddApiClients_WhenStsEnabled_SendsBearerTokenInsteadOfBasicHeader()
    {
        var stub = new StubHttpMessageHandler(HttpStatusCode.OK);
        var stsProvider = StsProviderReturning("jwt-token");
        var client = BuildClient(stub, stsProvider, stsEnabled: true);

        await client.GetAsync("/resource", TestContext.Current.CancellationToken);

        var authorization = stub.Requests.Should().ContainSingle().Which.Headers.Authorization;
        authorization!.Scheme.Should().Be("Bearer");
        authorization.Parameter.Should().Be("jwt-token");
        stsProvider.Verify(x => x.GetCredentialsAsync("cads-data-service", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task AddApiClients_WhenStsDisabled_SendsBasicHeaderAndDoesNotRequestToken()
    {
        var stub = new StubHttpMessageHandler(HttpStatusCode.OK);
        var stsProvider = StsProviderReturning("jwt-token");
        var client = BuildClient(stub, stsProvider, stsEnabled: false);

        await client.GetAsync("/resource", TestContext.Current.CancellationToken);

        var authorization = stub.Requests.Should().ContainSingle().Which.Headers.Authorization;
        authorization!.Scheme.Should().Be("Basic");
        authorization.Parameter.Should().Be("basic-key");
        stsProvider.Verify(x => x.GetCredentialsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ResilienceHandler_WhenStsEnabled_RequestsTokenOnEveryAttempt()
    {
        var stub = new StubHttpMessageHandler(HttpStatusCode.ServiceUnavailable);
        var stsProvider = StsProviderReturning("jwt-token");
        var client = BuildClient(stub, stsProvider, stsEnabled: true, retries: 2);

        await client.GetAsync("/resource", TestContext.Current.CancellationToken);

        stub.Requests.Should().HaveCount(3).And.AllSatisfy(r => r.Headers.Authorization!.Scheme.Should().Be("Bearer"));
        stsProvider.Verify(x => x.GetCredentialsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
    }

    private static Mock<IAwsStsProvider> StsProviderReturning(string token)
    {
        var provider = new Mock<IAwsStsProvider>();
        provider.Setup(x => x.GetCredentialsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new IdentityToken(token, DateTimeOffset.UtcNow.AddMinutes(5)));
        return provider;
    }

    private static HttpClient BuildClient(HttpMessageHandler primaryHandler, Mock<IAwsStsProvider> stsProvider, bool stsEnabled, int retries = 1)
    {
        var services = CreateServices();
        var config = BuildConfig(new Dictionary<string, string?>
        {
            [$"ApiClients:{ClientName}:BaseUrl"] = "http://test-api",
            [$"ApiClients:{ClientName}:BasicApiKey"] = "basic-key",
            [$"ApiClients:{ClientName}:Sts:Enabled"] = stsEnabled.ToString(),
            [$"ApiClients:{ClientName}:Sts:Audience"] = "cads-data-service",
            [$"ApiClients:{ClientName}:ResiliencePolicy:Retries"] = retries.ToString(),
            [$"ApiClients:{ClientName}:ResiliencePolicy:BaseDelaySeconds"] = "0",
            [$"ApiClients:{ClientName}:ResiliencePolicy:UseJitter"] = "false",
            [$"ApiClients:{ClientName}:ResiliencePolicy:TimeoutPeriodSeconds"] = "30"
        });

        services.AddSingleton(stsProvider.Object);
        services.AddApiClients(config);
        services.AddHttpClient(ClientName).ConfigurePrimaryHttpMessageHandler(() => primaryHandler);

        var sp = services.BuildServiceProvider();
        return sp.GetRequiredService<IHttpClientFactory>().CreateClient(ClientName);
    }

    private static HttpClient BuildResilientClient(HttpMessageHandler primaryHandler, int retries)
    {
        var services = CreateServices();
        var config = BuildConfig(new Dictionary<string, string?>
        {
            [$"ApiClients:{ClientName}:BaseUrl"] = "http://test-api",
            [$"ApiClients:{ClientName}:HealthcheckEnabled"] = "false",
            [$"ApiClients:{ClientName}:ResiliencePolicy:Retries"] = retries.ToString(),
            [$"ApiClients:{ClientName}:ResiliencePolicy:BaseDelaySeconds"] = "0",
            [$"ApiClients:{ClientName}:ResiliencePolicy:UseJitter"] = "false",
            [$"ApiClients:{ClientName}:ResiliencePolicy:TimeoutPeriodSeconds"] = "30"
        });

        services.AddApiClients(config);
        services.AddHttpClient(ClientName).ConfigurePrimaryHttpMessageHandler(() => primaryHandler);

        var sp = services.BuildServiceProvider();
        return sp.GetRequiredService<IHttpClientFactory>().CreateClient(ClientName);
    }
}