using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
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
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.Authentication.Handlers;

public class AwsStsInboundAuthenticationHandlerTests
{
    private const string Audience = "https://cads-bridge.test";
    private const string OrgId = "test-origin-Id";
    private const string Environment = "Dev";
    private const string StsClaim = "https://sts.amazonaws.com/";

    private readonly AclOptions _aclOptions = new()
    {
        StsClients = { ["cads-data-service"] = new AclOptions.StsClient { Scopes = ["imports", "other"] } }
    };

    private readonly AuthenticationConfiguration _authConfig;
    private readonly FakeIssuer _fakeIssuer;
    private readonly Mock<IHttpClientFactory> _httpClientFactory = new();

    // Each test gets its own issuer, because the handler caches discovery per issuer.
    private readonly string _issuer = $"https://sts-{Guid.NewGuid():N}.test";

    public AwsStsInboundAuthenticationHandlerTests()
    {
        _fakeIssuer = new FakeIssuer(_issuer);
        _httpClientFactory.Setup(x => x.CreateClient("proxy")).Returns(() => new HttpClient(_fakeIssuer));
        _authConfig = new AuthenticationConfiguration
        {
            AwsSts = new AwsStsConfiguration
            {
                Enabled = true,
                Audience = Audience,
                TrustedOrgId = OrgId,
                Environment = Environment
            }
        };
    }

    private async Task<AuthenticateResult> Authenticate(string? authorization, Endpoint? endpoint = null)
    {
        var optionsMonitor = new Mock<IOptionsMonitor<AuthenticationSchemeOptions>>();
        optionsMonitor.Setup(x => x.Get(AuthenticationConstants.AwsStsSchemeName)).Returns(new AuthenticationSchemeOptions());
        var subject = new AwsStsInboundAuthenticationHandler(
            optionsMonitor.Object, NullLoggerFactory.Instance, new UrlTestEncoder(),
            new OptionsWrapper<AuthenticationConfiguration>(_authConfig),
            new OptionsWrapper<AclOptions>(_aclOptions), _httpClientFactory.Object);

        var context = new DefaultHttpContext();
        if (authorization != null) context.Request.Headers.Authorization = authorization;
        if (endpoint != null) context.SetEndpoint(endpoint);

        await subject.InitializeAsync(
            new AuthenticationScheme(AuthenticationConstants.AwsStsSchemeName, null,
                typeof(AwsStsInboundAuthenticationHandler)),
            context);
        return await subject.AuthenticateAsync();
    }

    private static Dictionary<string, object> DefaultStsClaims(string org = OrgId, string env = Environment,
        string? service = "cads-data-service")
    {
        var tags = new Dictionary<string, object> { ["Environment"] = env };
        if (service != null) tags["ServiceName"] = service;
        return new Dictionary<string, object> { ["org_id"] = org, ["principal_tags"] = tags };
    }

    private string Token(string? aud = Audience, object? sts = null, DateTime? expires = null,
        SigningCredentials? creds = null,
        string? issuer = null)
    {
        var claims = new Dictionary<string, object> { [StsClaim] = sts ?? DefaultStsClaims() };
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

    private Task<AuthenticateResult> AuthenticateToken(string token)
    {
        return Authenticate("Bearer " + token);
    }

    [Fact]
    public async Task ValidToken_ReturnsExactlyTheConfiguredScopes()
    {
        var result = await AuthenticateToken(Token());

        result.Succeeded.Should().BeTrue();
        result.Principal!.Identity!.Name.Should().Be("cads-data-service");
        result.Principal.FindAll("scope").Select(c => c.Value).Should().BeEquivalentTo("imports", "other");
    }

    [Fact]
    public async Task NoAuthorizationHeader_ReturnsNoResult()
    {
        (await Authenticate(null)).None.Should().BeTrue();
    }

    [Theory]
    [InlineData("Basic abc123")]
    [InlineData("Bearer")]
    [InlineData("not a header ;;;")]
    public async Task NotABearerToken_ReturnsNoResult(string header)
    {
        (await Authenticate(header)).None.Should().BeTrue();
    }

    [Fact]
    public async Task AnonymousEndpoint_ReturnsNoResult()
    {
        var endpoint = new Endpoint(null, new EndpointMetadataCollection(new AllowAnonymousAttribute()), "anon");
        (await Authenticate("Bearer " + Token(), endpoint)).None.Should().BeTrue();
    }

    [Fact]
    public async Task MalformedToken_Fails()
    {
        (await AuthenticateToken("not-a-jwt")).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task TokenWithoutIssuer_Fails()
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
    public async Task WrongAudience_Fails()
    {
        (await AuthenticateToken(Token("https://another-service.test"))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WrongOrganization_Fails()
    {
        (await AuthenticateToken(Token(sts: DefaultStsClaims("wrong-org-name")))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WrongEnvironment_Fails()
    {
        (await AuthenticateToken(Token(sts: DefaultStsClaims(env: "Prod")))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task EnvironmentIsComparedIgnoringCase()
    {
        var result = await AuthenticateToken(Token(sts: DefaultStsClaims(env: "dev")));
        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public async Task ServiceNotInAllowList_Fails()
    {
        (await AuthenticateToken(Token(sts: DefaultStsClaims(service: "unknown-service")))).Failure.Should()
            .NotBeNull();
    }

    [Fact]
    public async Task MissingServiceName_Fails()
    {
        (await AuthenticateToken(Token(sts: DefaultStsClaims(service: null)))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task MissingStsClaim_Fails()
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
    public async Task StsClaimThatIsNotAnObject_Fails()
    {
        (await AuthenticateToken(Token(sts: "just-a-string"))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task ExpiredToken_Fails()
    {
        (await AuthenticateToken(Token(expires: DateTime.UtcNow.AddMinutes(-10)))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task BadSignature_Fails()
    {
        using var rsa = RSA.Create(2048);
        var other = new SigningCredentials(new RsaSecurityKey(rsa) { KeyId = FakeIssuer.KeyId },
            SecurityAlgorithms.RsaSha256);

        (await AuthenticateToken(Token(creds: other))).Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task HttpIssuerIsRejectedWhenHttpsIsRequired()
    {
        var result = await AuthenticateToken(Token(issuer: "http://cads-sts-mock"));
        result.Failure.Should().NotBeNull();
    }

    private sealed class FakeIssuer : HttpMessageHandler
    {
        public const string KeyId = "test-key";
        private readonly string _issuer;
        private readonly string _kid = KeyId;
        private readonly RSA _rsa = RSA.Create(2048);

        public FakeIssuer(string issuer)
        {
            _issuer = issuer;
        }

        public SigningCredentials Credentials =>
            new(new RsaSecurityKey(_rsa) { KeyId = _kid }, SecurityAlgorithms.RsaSha256);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var path = request.RequestUri!.AbsolutePath;
            string body;
            if (path.EndsWith("/.well-known/openid-configuration"))
            {
                body = JsonSerializer.Serialize(new { issuer = _issuer, jwks_uri = _issuer + "/jwks" });
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
                            kid = _kid,
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