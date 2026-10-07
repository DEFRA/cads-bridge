using CadsBridge.Infrastructure.Authentication.Configuration;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;

namespace CadsBridge.Infrastructure.Authentication.Handlers;

public class BasicAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder,
    IOptions<AclOptions> aclOptions
) : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        // Skip if endpoint allows anonymous
        var endpoint = Context.GetEndpoint();
        if (endpoint?.Metadata.GetMetadata<IAllowAnonymous>() != null)
            return NoResult();

        // No Authorization header
        if (!Request.Headers.TryGetValue("Authorization", out var headerValue))
            return NoResult();

        if (!AuthenticationHeaderValue.TryParse(headerValue, out var header))
            return Fail();

        if (!string.Equals(header.Scheme, AuthenticationConstants.ApiKeySchemeName, StringComparison.OrdinalIgnoreCase))
            return NoResult();

        // Decode Basic credentials
        var parameter = header.Parameter ?? string.Empty;
        var bytes = new byte[parameter.Length];
        if (!Convert.TryFromBase64String(parameter, bytes, out var written))
            return Fail();

        var parts = Encoding.UTF8.GetString(bytes, 0, written).Split(':', 2);

        if (parts.Length != 2)
            return Fail();

        var clientId = parts[0];
        var secret = parts[1];

        // Validate against ACL
        if (!aclOptions.Value.Clients.TryGetValue(clientId, out var client))
            return Fail();

        if (!SecretsMatch(client.Secret, secret))
            return Fail();

        // Build claims
        var claims = new List<Claim>
        {
            new(ClaimTypes.Name, clientId)
        };

        claims.AddRange(client.Scopes.Select(s => new Claim("scope", s)));

        var identity = new ClaimsIdentity(claims, Scheme.Name);
        var principal = new ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, Scheme.Name);

        return Success(ticket);
    }

    private static Task<AuthenticateResult> NoResult() =>
        Task.FromResult(AuthenticateResult.NoResult());

    private static Task<AuthenticateResult> Fail() =>
        Task.FromResult(AuthenticateResult.Fail("Invalid API key"));

    private static Task<AuthenticateResult> Success(AuthenticationTicket ticket) =>
        Task.FromResult(AuthenticateResult.Success(ticket));

    private static bool SecretsMatch(string expected, string supplied) =>
        CryptographicOperations.FixedTimeEquals(
            SHA256.HashData(Encoding.UTF8.GetBytes(expected)),
            SHA256.HashData(Encoding.UTF8.GetBytes(supplied)));
}