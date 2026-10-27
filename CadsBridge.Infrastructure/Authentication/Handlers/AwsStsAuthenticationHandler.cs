using CadsBridge.Infrastructure.Authentication.Configuration;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;
using System.Collections.Concurrent;
using System.IdentityModel.Tokens.Jwt;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace CadsBridge.Infrastructure.Authentication.Handlers;

public class AwsStsAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder,
    IOptions<AuthenticationConfiguration> authConfig,
    IOptions<AclOptions> aclOptions,
    IHttpClientFactory httpClientFactory
) : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    public const string StsClaimType = "https://sts.amazonaws.com/";

    private static readonly ConcurrentDictionary<string, ConfigurationManager<OpenIdConnectConfiguration>>
        s_configManagers = new();

    private readonly JwtSecurityTokenHandler _tokenHandler = new();

    protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (Context.GetEndpoint()?.Metadata.GetMetadata<IAllowAnonymous>() != null)
            return AuthenticateResult.NoResult();

        var token = GetBearerToken();
        if (token is null)
            return AuthenticateResult.NoResult();

        var config = authConfig.Value.AwsSts;

        if (!_tokenHandler.CanReadToken(token))
            return AuthenticateResult.Fail("Malformed token");

        var issuer = _tokenHandler.ReadJwtToken(token).Issuer;
        if (string.IsNullOrWhiteSpace(issuer))
            return AuthenticateResult.Fail("Token has no issuer");

        // Checked before any discovery fetch: the issuer is attacker-controlled until the signature is verified
        if (!config.TrustedIssuers.Contains(issuer, StringComparer.Ordinal))
            return AuthenticateResult.Fail("Token issuer is not trusted");

        ClaimsPrincipal principal;
        try
        {
            var discovery = await GetIssuerConfigurationAsync(issuer, config);
            principal = _tokenHandler.ValidateToken(token, new TokenValidationParameters
            {
                ValidIssuer = issuer,
                ValidateIssuer = true,
                ValidAudience = config.Audience, // rejects tokens not addressed to us
                ValidateAudience = true,
                ValidateLifetime = true,
                IssuerSigningKeys = discovery.SigningKeys,
                NameClaimType = "sub"
            }, out _);
        }
        catch (Exception ex) when (ex is SecurityTokenException or InvalidOperationException)
        {
            Logger.LogWarning(ex, "STS token validation failed for issuer {Issuer}", issuer);
            return AuthenticateResult.Fail("Invalid token");
        }

        if (!TryReadStsClaims(principal, out var stsClaims))
            return AuthenticateResult.Fail("Invalid STS claim");

        return MapCaller(stsClaims, config);
    }

    private string? GetBearerToken()
    {
        if (!Request.Headers.TryGetValue("Authorization", out var headerValue))
            return null;

        // TryParse instead of Parse, which throws (HTTP 500) on a malformed header.
        if (!AuthenticationHeaderValue.TryParse(headerValue.ToString(), out var header)
            || !string.Equals(header.Scheme, "Bearer", StringComparison.OrdinalIgnoreCase))
            return null;

        return header.Parameter;
    }

    private static bool TryReadStsClaims(ClaimsPrincipal principal, out JsonElement stsClaims)
    {
        stsClaims = default;
        var json = principal.FindFirst(StsClaimType)?.Value;
        if (string.IsNullOrEmpty(json))
            return true;

        try
        {
            stsClaims = JsonSerializer.Deserialize<JsonElement>(json);
            return true;
        }
        catch (JsonException)
        {
            // Without this the exception escapes as HTTP 500.
            return false;
        }
    }

    private AuthenticateResult MapCaller(JsonElement stsClaims, AwsStsConfiguration config)
    {
        var orgId = GetString(stsClaims, "org_id");
        var tags = stsClaims.ValueKind == JsonValueKind.Object && stsClaims.TryGetProperty("principal_tags", out var t)
            ? t
            : default;
        var serviceName = GetString(tags, "ServiceName");
        var environment = GetString(tags, "Environment");

        if (!string.Equals(orgId, config.TrustedOrgId, StringComparison.Ordinal))
            return AuthenticateResult.Fail($"Token is not from a trusted AWS organization ({orgId})");

        if (!string.Equals(environment, config.Environment, StringComparison.OrdinalIgnoreCase))
            return AuthenticateResult.Fail(
                $"Token minted for environment '{environment}', this service runs in '{config.Environment}'");

        if (serviceName is null)
            return AuthenticateResult.Fail("Caller serviceName is missing");

        if (!aclOptions.Value.StsClients.TryGetValue(serviceName, out var client))
            return AuthenticateResult.Fail($"Caller '{serviceName}' is not on the allow list");

        var claims = new List<Claim> { new(ClaimTypes.Name, serviceName) };
        // Literal "scope", as in BasicAuthenticationHandler.
        claims.AddRange(client.Scopes.Select(s => new Claim("scope", s)));

        var identity = new ClaimsIdentity(claims, Scheme.Name);
        return AuthenticateResult.Success(new AuthenticationTicket(new ClaimsPrincipal(identity), Scheme.Name));
    }

    private static string? GetString(JsonElement element, string propertyName)
    {
        // ValueKind check because GetString() throws (HTTP 500) on a non-string value
        return element.ValueKind == JsonValueKind.Object
               && element.TryGetProperty(propertyName, out var value)
               && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;
    }

    private Task<OpenIdConnectConfiguration> GetIssuerConfigurationAsync(string issuer, AwsStsConfiguration config)
    {
        var manager = s_configManagers.GetOrAdd(issuer, iss => new ConfigurationManager<OpenIdConnectConfiguration>(
            $"{iss.TrimEnd('/')}/.well-known/openid-configuration",
            new OpenIdConnectConfigurationRetriever(),
            new HttpDocumentRetriever(httpClientFactory.CreateClient("proxy"))
            {
                RequireHttps = config.RequireHttpsMetadata
            }));

        return manager.GetConfigurationAsync(CancellationToken.None);
    }
}