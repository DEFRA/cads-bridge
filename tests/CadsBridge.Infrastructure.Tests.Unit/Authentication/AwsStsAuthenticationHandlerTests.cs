using CadsBridge.Infrastructure.Authentication.Configuration;
using CadsBridge.Infrastructure.Authentication.Handlers;
using FluentAssertions;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.WebEncoders.Testing;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace CadsBridge.Infrastructure.Tests.Unit.Authentication;

public class AwsStsAuthenticationHandlerTests
{
    private const string Audience = "https://cads-bridge.test";
    private const string OrgId = "o-test";
    private const string Environment = "Dev";
    private const string ServiceName = "cads-data-service";

    private readonly AclOptions _aclOptions = new()
    {
        StsClients = { [ServiceName] = new AclOptions.StsClient { Scopes = ["imports", "other"] } }
    };

    private readonly AuthenticationConfiguration _authConfig;
    private readonly FakeIssuer _fakeIssuer;

    // Each test gets its own issuer, because the handler caches discovery per issuer.
    private readonly string _issuer = $"https://sts-{Guid.NewGuid():N}.test";

    private sealed class TestOptionsMonitor<T>(T value) : IOptionsMonitor<T>
    {
        public T CurrentValue => value;
        public T Get(string? name) => value;
        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }

    private sealed class FakeHttpClientFactory(HttpMessageHandler handler) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => new(handler, disposeHandler: false);
    }

    public AwsStsAuthenticationHandlerTests()
    {
        _fakeIssuer = new FakeIssuer(_issuer);
        _authConfig = new AuthenticationConfiguration
        {
            AwsSts = new AwsStsConfiguration
            {
                Enabled = true,
                Audience = Audience,
                TrustedOrgId = OrgId,
                Environment = Environment,
                TrustedIssuers = [_issuer]
            }
        };
    }

    private async Task<AuthenticateResult> Authenticate(string? authorization, Endpoint? endpoint = null)
    {
        var subject = new AwsStsAuthenticationHandler(
            new TestOptionsMonitor<AuthenticationSchemeOptions>(new AuthenticationSchemeOptions()),
            NullLoggerFactory.Instance,
            new UrlTestEncoder(),
            new OptionsWrapper<AuthenticationConfiguration>(_authConfig),
            new OptionsWrapper<AclOptions>(_aclOptions),
            new FakeHttpClientFactory(_fakeIssuer));

        var context = new DefaultHttpContext();
        if (authorization != null) context.Request.Headers.Authorization = authorization;
        if (endpoint != null) context.SetEndpoint(endpoint);

        await subject.InitializeAsync(
            new AuthenticationScheme(AuthenticationConstants.AwsStsSchemeName, null, typeof(AwsStsAuthenticationHandler)),
            context);
        return await subject.AuthenticateAsync();
    }

    private Task<AuthenticateResult> AuthenticateToken(string token) => Authenticate("Bearer " + token);

    private static Dictionary<string, object> DefaultStsClaims(string org = OrgId, string env = Environment,
        string? service = ServiceName)
    {
        var tags = new Dictionary<string, object> { ["Environment"] = env };
        if (service != null) tags["ServiceName"] = service;
        return new Dictionary<string, object> { ["org_id"] = org, ["principal_tags"] = tags };
    }

    private string Token(string? aud = Audience, object? sts = null, DateTime? expires = null,
        SigningCredentials? creds = null, string? issuer = null)
    {
        var claims = new Dictionary<string, object> { [AwsStsAuthenticationHandler.StsClaimType] = sts ?? DefaultStsClaims() };
        var now = DateTime.UtcNow;
        return new JwtSecurityTokenHandler().CreateEncodedJwt(new SecurityTokenDescriptor
        {
            Issuer = issuer ?? _issuer,
            Audience = aud,
            Claims = claims,
            NotBefore = now.AddMinutes(-20),
            IssuedAt = now.AddMinutes(-20),
            Expires = expires ?? now.AddMinutes(5),
            SigningCredentials = creds ?? _fakeIssuer.Credentials
        });
    }

    [Fact]
    public async Task WhenValidToken_ShouldReturnExactlyTheConfiguredScopes()
    {
        var result = await AuthenticateToken(Token());

        result.Succeeded.Should().BeTrue();
        result.Principal!.Identity!.Name.Should().Be(ServiceName);
        result.Principal.Identity.AuthenticationType.Should().Be(AuthenticationConstants.AwsStsSchemeName);
        result.Principal.FindAll("scope").Select(c => c.Value).Should().BeEquivalentTo("imports", "other");
    }

    [Fact]
    public async Task WhenNoAuthorizationHeader_ShouldReturnNoResult()
    {
        (await Authenticate(null)).None.Should().BeTrue();
    }

    [Theory]
    [InlineData("Basic abc123")]
    [InlineData("Bearer")]
    [InlineData("not a header ;;;")]
    public async Task WhenNotABearerToken_ShouldReturnNoResult(string header)
    {
        (await Authenticate(header)).None.Should().BeTrue();
    }

    [Fact]
    public async Task WhenAnonymousEndpoint_ShouldReturnNoResult()
    {
        var endpoint = new Endpoint(null, new EndpointMetadataCollection(new AllowAnonymousAttribute()), "anon");
        (await Authenticate("Bearer " + Token(), endpoint)).None.Should().BeTrue();
    }

    [Fact]
    public async Task WhenMalformedToken_ShouldFail()
    {
        (await AuthenticateToken("not-a-jwt")).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenTokenWithoutIssuer_ShouldFail()
    {
        var token = new JwtSecurityTokenHandler().CreateEncodedJwt(new SecurityTokenDescriptor
        {
            Audience = Audience,
            Expires = DateTime.UtcNow.AddMinutes(5),
            SigningCredentials = _fakeIssuer.Credentials
        });
        (await AuthenticateToken(token)).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenIssuerNotTrusted_ShouldFailWithoutFetchingDiscovery()
    {
        // Validly signed by a key the issuer would serve, but the issuer isn't on the allow list
        var untrusted = new FakeIssuer($"https://sts-{Guid.NewGuid():N}.test");

        var result = await AuthenticateToken(Token(issuer: untrusted.Issuer, creds: untrusted.Credentials));

        result.Failure.Should().NotBeNull();
        _fakeIssuer.RequestCount.Should().Be(0);
        untrusted.RequestCount.Should().Be(0);
    }

    [Fact]
    public async Task WhenWrongAudience_ShouldFail()
    {
        (await AuthenticateToken(Token("https://another-service.test"))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenWrongOrganization_ShouldFail()
    {
        (await AuthenticateToken(Token(sts: DefaultStsClaims("o-wrong")))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenWrongEnvironment_ShouldFail()
    {
        (await AuthenticateToken(Token(sts: DefaultStsClaims(env: "Prod")))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenEnvironmentDiffersOnlyByCase_ShouldSucceed()
    {
        (await AuthenticateToken(Token(sts: DefaultStsClaims(env: "dev")))).Succeeded.Should().BeTrue();
    }

    [Fact]
    public async Task WhenServiceNotInAllowList_ShouldFail()
    {
        (await AuthenticateToken(Token(sts: DefaultStsClaims(service: "unknown-service")))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenServiceNameMissing_ShouldFail()
    {
        (await AuthenticateToken(Token(sts: DefaultStsClaims(service: null)))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenStsClaimMissing_ShouldFail()
    {
        var token = new JwtSecurityTokenHandler().CreateEncodedJwt(new SecurityTokenDescriptor
        {
            Issuer = _issuer,
            Audience = Audience,
            Expires = DateTime.UtcNow.AddMinutes(5),
            SigningCredentials = _fakeIssuer.Credentials
        });
        (await AuthenticateToken(token)).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenStsClaimIsNotAnObject_ShouldFail()
    {
        (await AuthenticateToken(Token(sts: "just-a-string"))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenStsClaimValueIsNotAString_ShouldFail()
    {
        var sts = DefaultStsClaims();
        sts["org_id"] = 12345;

        (await AuthenticateToken(Token(sts: sts))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenTokenExpired_ShouldFail()
    {
        (await AuthenticateToken(Token(expires: DateTime.UtcNow.AddMinutes(-10)))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenBadSignature_ShouldFail()
    {
        using var rsa = RSA.Create(2048);
        var other = new SigningCredentials(new RsaSecurityKey(rsa) { KeyId = FakeIssuer.KeyId }, SecurityAlgorithms.RsaSha256);

        (await AuthenticateToken(Token(creds: other))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenHttpIssuerAndHttpsRequired_ShouldFail()
    {
        const string httpIssuer = "http://cads-sts-mock";
        _authConfig.AwsSts.TrustedIssuers = [httpIssuer];

        (await AuthenticateToken(Token(issuer: httpIssuer))).Failure.Should().NotBeNull();
    }

    private sealed class FakeIssuer(string issuer) : HttpMessageHandler
    {
        public const string KeyId = "test-key";
        private readonly RSA _rsa = RSA.Create(2048);

        public string Issuer => issuer;
        public int RequestCount { get; private set; }

        public SigningCredentials Credentials =>
            new(new RsaSecurityKey(_rsa) { KeyId = KeyId }, SecurityAlgorithms.RsaSha256);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            RequestCount++;

            var path = request.RequestUri!.AbsolutePath;
            string body;
            if (path.EndsWith("/.well-known/openid-configuration"))
            {
                body = JsonSerializer.Serialize(new { issuer, jwks_uri = issuer + "/jwks" });
            }
            else if (path.EndsWith("/jwks"))
            {
                var p = _rsa.ExportParameters(false);
                body = JsonSerializer.Serialize(new
                {
                    keys = new[]
                    {
                        new
                        {
                            kty = "RSA",
                            use = "sig",
                            alg = "RS256",
                            kid = KeyId,
                            n = Base64UrlEncoder.Encode(p.Modulus),
                            e = Base64UrlEncoder.Encode(p.Exponent)
                        }
                    }
                });
            }
            else
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound));
            }

            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(body, Encoding.UTF8, "application/json")
            });
        }
    }
}