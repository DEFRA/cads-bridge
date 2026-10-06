using System.Security.Claims;
using System.Text.Encodings.Web;
using CadsBridge.Application.Identity;
using CadsBridge.Infrastructure.Authentication.Configuration;
using CadsBridge.Testing.Support.Constants;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Testing.Support.Fakes.Authentication;

public class FakeJwtHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder,
    IOptionsMonitor<AuthenticationConfiguration> authConfig) : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var authorizationHeader = Request.Headers.Authorization.ToString();
        if (!authorizationHeader.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
        {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        var claims = new List<Claim>
        {
            new(ClaimTypes.Name, TestAuthConstants.AzureAdCadsUsername),
            new(ClaimTypes.Email, TestAuthConstants.AzureAdCadsEmail),
            new("name", TestAuthConstants.AzureAdCadsUsername)
        };

        if (Scheme.Name == AuthenticationConstants.AzureADSchemeName)
        {
            var azureAd = authConfig.CurrentValue.AzureAD;

            claims.Add(new Claim(CustomClaimTypes.Oid, Guid.NewGuid().ToString()));
            claims.Add(new Claim(CustomClaimTypes.TenantId, "test-aad-tenant"));

            var token = authorizationHeader["Bearer ".Length..];
            if (token != TestAuthConstants.FakeJwtMissingSqsAdminScope)
            {
                claims.Add(new Claim(azureAd.ScopeClaimType, ScopeNames.SqsAdminManager));
            }
        }

        var identity = new ClaimsIdentity(claims, Scheme.Name);
        var principal = new ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, Scheme.Name);

        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}