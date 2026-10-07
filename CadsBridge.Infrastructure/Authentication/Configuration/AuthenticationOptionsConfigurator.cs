using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

namespace CadsBridge.Infrastructure.Authentication.Configuration;

public class AuthenticationOptionsConfigurator : IConfigureOptions<AuthenticationOptions>
{
    public void Configure(AuthenticationOptions options)
    {
        // Neutral fallback; policies decide the real scheme
        options.DefaultScheme = AuthenticationConstants.ApiKeySchemeName;
        options.DefaultAuthenticateScheme = AuthenticationConstants.ApiKeySchemeName;
        options.DefaultChallengeScheme = AuthenticationConstants.ApiKeySchemeName;
    }
}