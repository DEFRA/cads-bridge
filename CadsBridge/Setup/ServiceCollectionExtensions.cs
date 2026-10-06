using CadsBridge.Application.Setup;
using CadsBridge.Infrastructure.Authentication.Configuration;
using CadsBridge.Infrastructure.Authentication.Handlers;
using CadsBridge.Infrastructure.Configuration.Aws;
using CadsBridge.Infrastructure.Json;
using CadsBridge.Infrastructure.Setup;
using CadsBridge.Worker.Setup;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Options;
using System.IdentityModel.Tokens.Jwt;
using CadsBridge.Application.Identity;
using CadsBridge.Utils.Http;
using Microsoft.IdentityModel.Tokens;

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
        var authConfig = configuration.GetSection(nameof(AuthenticationConfiguration)).Get<AuthenticationConfiguration>()!;

        services.Configure<AclOptions>(
            configuration.GetSection("Acl"));

        services.Configure<AuthenticationConfiguration>(
            configuration.GetSection(nameof(AuthenticationConfiguration)));

        services.AddSingleton<IConfigureOptions<AuthenticationOptions>, AuthenticationOptionsConfigurator>();

        JwtSecurityTokenHandler.DefaultInboundClaimTypeMap.Clear();

        var authenticationBuilder = services.AddAuthentication();
        var authorizationBuilder = services.AddAuthorizationBuilder();

        if (authConfig.ApiKey.Enabled)
        {
            authenticationBuilder.AddApiKeyScheme();
            authorizationBuilder.AddApiKeyPolicy();
        }

        if (authConfig.AzureAD.Enabled)
        {
            authenticationBuilder.AddAzureAdScheme(authConfig.AzureAD);
            authorizationBuilder.AddAzureAdPolicies(authConfig);
        }
    }

    private static void AddApiKeyScheme(this AuthenticationBuilder authenticationBuilder)
    {
        authenticationBuilder.AddScheme<AuthenticationSchemeOptions, BasicAuthenticationHandler>(
            AuthenticationConstants.ApiKeySchemeName, _ => { });
    }

    private static void AddApiKeyPolicy(this AuthorizationBuilder authorizationBuilder)
    {
        var apiKeyPolicy = new AuthorizationPolicyBuilder()
            .AddAuthenticationSchemes(AuthenticationConstants.ApiKeySchemeName)
            .RequireAuthenticatedUser()
            .Build();

        authorizationBuilder.AddPolicy(AuthenticationConstants.ApiKeyPolicyName, apiKeyPolicy);
        authorizationBuilder.SetFallbackPolicy(apiKeyPolicy);
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

    private static void AddAzureAdPolicies(this AuthorizationBuilder authorizationBuilder, AuthenticationConfiguration authenticationConfiguration)
    {
        // Admin policies all same shape: Azure AD only, a specific scope, and the superuser role
        // We are just using the one at this time but leaving the structure in place for future expansion
        (string Policy, string Scope)[] adminPolicies =
        [
            (AuthenticationConstants.AadSqsAdminExecutePolicy, ScopeNames.SqsAdminManager)
        ];

        var scopeClaim = authenticationConfiguration.AzureAD.ScopeClaimType;

        foreach (var (name, scope) in adminPolicies)
        {
            authorizationBuilder.AddPolicy(name, policy => policy
                .AddAuthenticationSchemes(AuthenticationConstants.AzureADSchemeName)
                .RequireAuthenticatedUser()
                .RequireScope(scopeClaim, scope));
        }
    }

    private static AuthorizationPolicyBuilder RequireScope(
        this AuthorizationPolicyBuilder policy, string scopeClaimType, string scope) =>
        policy.RequireAssertion(ctx => ctx.User.Claims
            .Where(c => c.Type == scopeClaimType)
            .SelectMany(c => c.Value.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            .Contains(scope, StringComparer.Ordinal));
}