using FluentAssertions;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.WebEncoders.Testing;
using System.Security.Claims;
using System.Text;
using CadsBridge.Infrastructure.Authentication.Configuration;
using CadsBridge.Infrastructure.Authentication.Handlers;
using Microsoft.Extensions.Logging.Abstractions;

namespace CadsBridge.Infrastructure.Tests.Unit.Authentication;

public class BasicAuthenticationHandlerTests
{
    private readonly BasicAuthenticationHandler _subject;
    private readonly AclOptions _aclOptions = new();
    private sealed class TestOptionsMonitor<T>(T value) : IOptionsMonitor<T>
    {
        public T CurrentValue => value;
        public T Get(string? name) => value;
        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }
    private readonly TestOptionsMonitor<AuthenticationSchemeOptions> _optionsMonitor = new(new AuthenticationSchemeOptions());

    public BasicAuthenticationHandlerTests()
    {
        _subject = new BasicAuthenticationHandler(
            _optionsMonitor,
            NullLoggerFactory.Instance,
            new UrlTestEncoder(),
            new OptionsWrapper<AclOptions>(_aclOptions)
        );
    }

    private static AuthenticationScheme Scheme() =>
        new(AuthenticationConstants.ApiKeySchemeName, AuthenticationConstants.ApiKeySchemeName, typeof(BasicAuthenticationHandler));

    private async Task<AuthenticateResult> Authenticate(HttpContext context)
    {
        await _subject.InitializeAsync(Scheme(), context);
        return await _subject.AuthenticateAsync();
    }

    [Fact]
    public async Task WhenNoAuthorizationHeader_ShouldReturnNoResult()
    {
        var result = await Authenticate(new DefaultHttpContext());
        result.None.Should().BeTrue();
    }

    [Fact]
    public async Task WhenInvalidAuthorizationHeaderScheme_ShouldReturnNoResult()
    {
        var context = new DefaultHttpContext();
        context.Request.Headers.Authorization = "Bearer abc123";

        var result = await Authenticate(context);
        result.None.Should().BeTrue();
    }

    [Fact]
    public async Task WhenNoCredentials_ShouldFail()
    {
        var context = new DefaultHttpContext();
        context.Request.Headers.Authorization = "Basic ";

        var result = await Authenticate(context);
        result.Failure.Should().NotBeNull();
    }

    [Theory]
    [InlineData(":secret")]
    [InlineData("username:")]
    public async Task WhenInvalidCredentialsFormat_ShouldFail(string credentials)
    {
        var context = new DefaultHttpContext();
        context.Request.Headers.Authorization = $"Basic {Convert.ToBase64String(Encoding.UTF8.GetBytes(credentials))}";

        var result = await Authenticate(context);
        result.Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenClientIdNotFound_ShouldFail()
    {
        _aclOptions.Clients.Add("Different", new AclOptions.ApiKeyClient
        {
            Secret = "secret",
            Scopes = []
        });

        var context = new DefaultHttpContext();
        context.Request.Headers.Authorization = $"Basic {Convert.ToBase64String("client:secret"u8.ToArray())}";

        var result = await Authenticate(context);
        result.Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenSecretDoesNotMatch_ShouldFail()
    {
        _aclOptions.Clients.Add("client", new AclOptions.ApiKeyClient
        {
            Secret = "different-secret",
            Scopes = []
        });

        var context = new DefaultHttpContext();
        context.Request.Headers.Authorization = $"Basic {Convert.ToBase64String("client:secret"u8.ToArray())}";

        var result = await Authenticate(context);
        result.Failure.Should().NotBeNull();
    }

    [Fact]
    public async Task WhenValidCredentials_ShouldSucceed()
    {
        _aclOptions.Clients.Add("client", new AclOptions.ApiKeyClient
        {
            Secret = "secret",
            Scopes = ["access", "read"]
        });

        var context = new DefaultHttpContext();
        context.Request.Headers.Authorization = $"Basic {Convert.ToBase64String("client:secret"u8.ToArray())}";

        var result = await Authenticate(context);

        result.Succeeded.Should().BeTrue();
        result.Principal?.Identity!.Name.Should().Be("client");

        var claims = result.Principal?.Claims.ToList();
        claims.Should().Contain(c => c.Type == ClaimTypes.Name && c.Value == "client");
        claims.Should().Contain(c => c.Type == "scope" && c.Value == "access");
        claims.Should().Contain(c => c.Type == "scope" && c.Value == "read");
    }

    [Fact]
    public async Task WhenEndpointAllowsAnonymous_ShouldReturnNoResult()
    {
        var context = new DefaultHttpContext();

        var endpoint = new Endpoint(
            requestDelegate: _ => Task.CompletedTask,
            metadata: new EndpointMetadataCollection(new AllowAnonymousAttribute()),
            displayName: "Test"
        );

        context.SetEndpoint(endpoint);

        var result = await Authenticate(context);
        result.None.Should().BeTrue();
    }
}