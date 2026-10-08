using CadsBridge.Application.Identity;
using CadsBridge.Infrastructure.Authentication.Configuration;
using CadsBridge.Testing.Support.Constants;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Security.Claims;
using System.Text.Encodings.Web;

namespace CadsBridge.Testing.Support.Fakes.Authentication;

public class FakeApiKeyHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder)
    : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var claims = new List<Claim>
        {
            new(ClaimTypes.Name, TestAuthConstants.AzureAdCadsUsername),
            new(ClaimTypes.Email, TestAuthConstants.AzureAdCadsEmail),
            new("name", TestAuthConstants.AzureAdCadsUsername),
            new(CustomClaimTypes.Oid, Guid.NewGuid().ToString()),
            new(CustomClaimTypes.TenantId, "test-internal-tenant"),
        };

        var identity = new ClaimsIdentity(
            claims,
            AuthenticationConstants.ApiKeySchemeName
        );

        var principal = new ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, AuthenticationConstants.ApiKeySchemeName);

        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}