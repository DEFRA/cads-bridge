using System.Net;
using CadsBridge.Infrastructure.ApiClients.Setup;
using CadsBridge.Testing.Support.Utilities.Http;
using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.ApiClients.Health;

public class ApiClientHealthCheckTests
{
    private const string HttpClientName = "test-client-health"; // probe client
    private const string DisplayName = "test-client";           // reporting name
    private readonly Mock<IHttpClientFactory> _httpClientFactory = new();

    private ApiClientHealthCheck CreateSut(HttpMessageHandler handler)
    {
        var client = new HttpClient(handler) { BaseAddress = new Uri("http://test-api") };
        _httpClientFactory.Setup(x => x.CreateClient(HttpClientName)).Returns(client);
        return new ApiClientHealthCheck(_httpClientFactory.Object, HttpClientName, DisplayName);
    }

    [Fact]
    public async Task ShouldReturnHealthy_WhenEndpointReturnsSuccess()
    {
        var sut = CreateSut(new StubHttpMessageHandler(HttpStatusCode.OK));

        var result = await sut.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Healthy);
        result.Data["client-name"].Should().Be(DisplayName);   // reports the display name
        result.Data["status-code"].Should().Be(HttpStatusCode.OK);
    }

    [Theory]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.BadRequest)]
    public async Task ShouldReturnDegraded_WhenEndpointReturnsNonSuccess(HttpStatusCode statusCode)
    {
        var sut = CreateSut(new StubHttpMessageHandler(statusCode));

        var result = await sut.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Degraded);
        result.Data["status-code"].Should().Be(statusCode);
    }

    [Fact]
    public async Task ShouldReturnUnhealthy_WhenRequestThrows()
    {
        var sut = CreateSut(new StubHttpMessageHandler(new HttpRequestException("connection refused")));

        var result = await sut.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Data.Should().ContainKey("error");
    }

    [Fact]
    public async Task ShouldReturnUnhealthy_WithTimeoutMessage_WhenRequestTimesOut()
    {
        var sut = CreateSut(new StubHttpMessageHandler(new TaskCanceledException()));

        var result = await sut.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Exception.Should().BeOfType<TimeoutException>();
    }


}