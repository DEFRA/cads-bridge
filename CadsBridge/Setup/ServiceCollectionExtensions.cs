using CadsBridge.Application.Identity;
using CadsBridge.Application.Setup;
using CadsBridge.Infrastructure.Authentication.Configuration;
using CadsBridge.Infrastructure.Authentication.Handlers;
using CadsBridge.Infrastructure.Configuration.Aws;
using CadsBridge.Infrastructure.Json;
using CadsBridge.Infrastructure.Setup;
using CadsBridge.Utils.Http;
using CadsBridge.Worker.Setup;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;

namespace CadsBridge.Setup;

public static class ServiceCollectionExtensions
{
    public static void ConfigureCadsBridge(this IServiceCollection services, IConfiguration configuration)
    {
        services.ConfigureAuthentication(configuration);

        services.AddControllers()
            .AddJsonOptions(opts =>
            {
                opts.JsonSerializerOptions.PropertyNamingPolicy = JsonDefaults.DefaultOptions.PropertyNamingPolicy;
                opts.JsonSerializerOptions.WriteIndented = JsonDefaults.DefaultOptions.WriteIndented;
                foreach (var converter in JsonDefaults.DefaultOptions.Converters)
                {
                    opts.JsonSerializerOptions.Converters.Add(converter);
                }
            });

        services.AddDefaultAWSOptions(configuration.GetAWSOptions());
        services.Configure<AwsConfig>(configuration.GetSection(AwsConfig.SectionName));

        services.AddInfrastructureLayer(configuration);

        services.AddBackgroundServiceScheduling(configuration);

        services.AddApplicationLayer();
    }

    private static void ConfigureAuthentication(this IServiceCollection services, IConfiguration configuration)
    {
        var schemes = new List<string>();

        var authConfig = configuration.GetSection(nameof(AuthenticationConfiguration)).Get<AuthenticationConfiguration>()!;

        services.Configure<AclOptions>(
            configuration.GetSection("Acl"));

        services.Configure<AuthenticationConfiguration>(
            configuration.GetSection(nameof(AuthenticationConfiguration)));

        services.AddSingleton<IConfigureOptions<AuthenticationOptions>, AuthenticationOptionsConfigurator>();

        JwtSecurityTokenHandler.DefaultInboundClaimTypeMap.Clear();

        var authenticationBuilder = services.AddAuthentication();

        if (authConfig.ApiKey.Enabled)
        {
            authenticationBuilder.AddApiKeyScheme();
            schemes.Add(AuthenticationConstants.ApiKeySchemeName);
        }

        if (authConfig.AzureAD.Enabled)
        {
            authenticationBuilder.AddAzureAdScheme(authConfig.AzureAD);
            schemes.Add(AuthenticationConstants.AzureADSchemeName);
        }

        if (authConfig.AwsSts.Enabled)
        {
            authenticationBuilder.AddAwsStsScheme();
            schemes.Add(AuthenticationConstants.AwsStsSchemeName);
        }

        if (schemes.Count == 0)
        {
            throw new InvalidOperationException("At least one authentication scheme must be enabled.");
        }

        services.AddAuthorisationPolicies(authConfig, schemes);
    }

    private static void AddApiKeyScheme(this AuthenticationBuilder authenticationBuilder)
    {
        authenticationBuilder.AddScheme<AuthenticationSchemeOptions, BasicAuthenticationHandler>(
            AuthenticationConstants.ApiKeySchemeName, _ => { });
    }

    private static void AddAwsStsScheme(this AuthenticationBuilder authenticationBuilder)
    {
        authenticationBuilder.AddScheme<AuthenticationSchemeOptions, AwsStsAuthenticationHandler>(
            AuthenticationConstants.AwsStsSchemeName, _ => { });
    }

    private static void AddAzureAdScheme(this AuthenticationBuilder authenticationBuilder, AuthenticationProviderConfiguration authenticationProviderConfiguration)
    {
        authenticationBuilder.AddJwtBearer(AuthenticationConstants.AzureADSchemeName, options =>
        {
            options.MapInboundClaims = false;

            if (!string.IsNullOrWhiteSpace(authenticationProviderConfiguration.MetadataAddress))
            {
                options.MetadataAddress = authenticationProviderConfiguration.MetadataAddress;
            }
            else if (!string.IsNullOrWhiteSpace(authenticationProviderConfiguration.Authority))
            {
                options.Authority = authenticationProviderConfiguration.Authority;
            }

            options.RequireHttpsMetadata = authenticationProviderConfiguration.RequireHttpsMetadata;
            options.Audience = authenticationProviderConfiguration.Audience;

            options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateIssuer = authenticationProviderConfiguration.ValidateIssuer,
                ValidateAudience = true,
                ValidAudience = authenticationProviderConfiguration.Audience,
                ValidateLifetime = true,
                ValidateIssuerSigningKey = true,
                NameClaimType = "name",
                RoleClaimType = authenticationProviderConfiguration.RoleClaimType,
                ValidTypes = ["JWT", "at+jwt"]
            };

            options.BackchannelHttpHandler = new ProxyHttpMessageHandler();
        });
    }

    private static void AddAuthorisationPolicies(
        this IServiceCollection services,
        AuthenticationConfiguration authConfig,
        List<string> schemes)
    {
        var builder = services.AddAuthorizationBuilder();

        builder.AddPolicy(AuthenticationConstants.DefaultPolicy, p => p
            .AddAuthenticationSchemes([.. schemes])
            .RequireAuthenticatedUser());

        (string Policy, string Scope)[] customScopePolicies =
        [
            (AuthenticationConstants.AadSqsAdminExecutePolicy, ScopeNames.SqsAdminManager)
        ];

        foreach (var (name, scope) in customScopePolicies)
        {
            builder.AddPolicy(name, policy => policy.RequireEnabledSchemes(authConfig)
                .RequireScopeFromEnabledSchemes(authConfig, scope));
        }

        builder.SetFallbackPolicy(new AuthorizationPolicyBuilder()
            .AddAuthenticationSchemes([.. schemes])
            .RequireAuthenticatedUser()
            .Build());
    }

    private static AuthorizationPolicyBuilder RequireEnabledSchemes(
        this AuthorizationPolicyBuilder policy,
        AuthenticationConfiguration config)
    {
        return policy
            .AddSchemeIf(config.ApiKey.Enabled, AuthenticationConstants.ApiKeySchemeName)
            .AddSchemeIf(config.AzureAD.Enabled, AuthenticationConstants.AzureADSchemeName)
            .AddSchemeIf(config.AwsSts.Enabled, AuthenticationConstants.AwsStsSchemeName)
            .RequireAuthenticatedUser();
    }

    private static AuthorizationPolicyBuilder AddSchemeIf(
        this AuthorizationPolicyBuilder policy, bool enabled, string scheme) =>
        enabled ? policy.AddAuthenticationSchemes(scheme) : policy;

    private static AuthorizationPolicyBuilder RequireScopeFromEnabledSchemes(
        this AuthorizationPolicyBuilder policy,
        AuthenticationConfiguration config,
        string scope)
    {
        return policy.RequireAssertion(ctx =>
        {
            var user = ctx.User;

            // Azure AD scopes
            if (config.AzureAD.Enabled &&
                HasScope(user, config.AzureAD.ScopeClaimType, scope))
                return true;

            // AWS STS scopes, granted per caller by Acl:StsClients. Only the STS identity is checked:
            // ApiKey identities use the same literal "scope" claim type.
            if (config.AwsSts.Enabled &&
                user.Identities.Any(i => i.AuthenticationType == AuthenticationConstants.AwsStsSchemeName &&
                                         HasScope(new ClaimsPrincipal(i), "scope", scope)))
                return true;

            return false;
        });
    }

    private static bool HasScope(ClaimsPrincipal user, string claimType, string scope) =>
        user.Claims
            .Where(c => c.Type == claimType)
            .SelectMany(c => c.Value.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            .Contains(scope, StringComparer.Ordinal);
}