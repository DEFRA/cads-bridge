using Amazon.Runtime;
using Amazon.SecurityToken.Model;
using CadsBridge.Infrastructure.ApiClients.Configuration;
using CadsBridge.Infrastructure.Authentication.Fakes;
using CadsBridge.Testing.Support.Utilities.Http;
using FluentAssertions;
using Moq;
using System.Net;
using System.Text;

namespace CadsBridge.Infrastructure.Tests.Unit.Authentication.Fakes;

public class MockSecurityTokenServiceClientTests
{
    private const string Audience = "https://cads-data-service.dev.cdp-int.defra.cloud";
    private const string TokenEndpoint = "http://cads-sts-mock/connect/token";
    private const string ClientId = "local-sts-wrong-org";
    private const string Sec = "sec";
    private const string Scope = "sts.cads-bridge";

    private readonly MutableTimeProvider _timeProvider = new(DateTimeOffset.Parse("2026-07-14T10:00:00Z"));

    [Fact]
    public async Task GetWebIdentityTokenAsync_ShouldPostClientCredentialsFormToTokenEndpoint()
    {
        string? body = null;
        var stub = new StubHttpMessageHandler((request, ct) =>
        {
            body = request.Content!.ReadAsStringAsync(ct).GetAwaiter().GetResult();
            return TokenResponse("jwt-token", 300);
        });

        await CreateSut(stub).GetWebIdentityTokenAsync(CreateRequest(), TestContext.Current.CancellationToken);

        var request = stub.Requests.Should().ContainSingle().Subject;
        request.Method.Should().Be(HttpMethod.Post);
        request.RequestUri.Should().Be(new Uri(TokenEndpoint));
        ParseForm(body!).Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = ClientId,
            ["client_secret"] = Sec,
            ["scope"] = Scope
        });
    }

    [Fact]
    public async Task GetWebIdentityTokenAsync_ShouldReturnAccessTokenWithExpiryFromExpiresIn()
    {
        var stub = new StubHttpMessageHandler((_, _) => TokenResponse("jwt-token", 300));

        var response = await CreateSut(stub).GetWebIdentityTokenAsync(CreateRequest(), TestContext.Current.CancellationToken);

        response.WebIdentityToken.Should().Be("jwt-token");
        response.Expiration.Should().Be(_timeProvider.GetUtcNow().UtcDateTime.AddSeconds(300));
        response.HttpStatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Theory]
    [InlineData(HttpStatusCode.BadRequest, ErrorType.Sender)]
    [InlineData(HttpStatusCode.ServiceUnavailable, ErrorType.Receiver)]
    public async Task GetWebIdentityTokenAsync_ShouldThrowAmazonServiceException_WhenMockReturnsError(HttpStatusCode statusCode, ErrorType errorType)
    {
        var stub = new StubHttpMessageHandler(statusCode);

        var act = () => CreateSut(stub).GetWebIdentityTokenAsync(CreateRequest(), TestContext.Current.CancellationToken);

        var ex = (await act.Should().ThrowAsync<AmazonServiceException>()).Which;
        ex.StatusCode.Should().Be(statusCode);
        ex.ErrorType.Should().Be(errorType);
    }

    [Fact]
    public async Task GetWebIdentityTokenAsync_ShouldThrowAmazonClientException_WhenMockUnreachable()
    {
        var stub = new StubHttpMessageHandler(new HttpRequestException("connection refused"));

        var act = () => CreateSut(stub).GetWebIdentityTokenAsync(CreateRequest(), TestContext.Current.CancellationToken);

        (await act.Should().ThrowAsync<AmazonClientException>())
            .Which.InnerException.Should().BeOfType<HttpRequestException>();
    }

    private MockSecurityTokenServiceClient CreateSut(HttpMessageHandler handler)
    {
        var factory = new Mock<IHttpClientFactory>();
        factory.Setup(x => x.CreateClient(MockSecurityTokenServiceClient.HttpClientName))
            .Returns(() => new HttpClient(handler, disposeHandler: false));

        var mockConfig = new StsMockConfig
        {
            Enabled = true,
            TokenEndpoint = TokenEndpoint,
            ClientId = ClientId,
            ClientSecret = Sec,
            Scope = Scope
        };

        return new MockSecurityTokenServiceClient(factory.Object, mockConfig, _timeProvider);
    }

    private static GetWebIdentityTokenRequest CreateRequest() => new()
    {
        Audience = [Audience],
        SigningAlgorithm = "RS256",
        DurationSeconds = 300
    };

    private static HttpResponseMessage TokenResponse(string accessToken, int expiresIn) => new(HttpStatusCode.OK)
    {
        Content = new StringContent(
            $$"""{"token_type":"Bearer","access_token":"{{accessToken}}","expires_in":{{expiresIn}}}""",
            Encoding.UTF8,
            "application/json")
    };

    private static Dictionary<string, string> ParseForm(string body) =>
        body.Split('&')
            .Select(pair => pair.Split('=', 2))
            .ToDictionary(kv => WebUtility.UrlDecode(kv[0]), kv => WebUtility.UrlDecode(kv[1]));

    private sealed class MutableTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}