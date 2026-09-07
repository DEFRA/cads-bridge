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
            configuration.GetSection("AuthenticationConfiguration"));
        var authenticationBuilder = services.AddAuthentication();
        var authorizationBuilder = services.AddAuthorizationBuilder();
        if (authConfig.ApiKey.Enabled)
        {
            AddApiKeyScheme(authenticationBuilder);
            authorizationBuilder.AddApiKeyPolicy();
        }
    }

    private static void AddApiKeyScheme(this AuthenticationBuilder authenticationBuilder)
    {
        authenticationBuilder.AddScheme<AuthenticationSchemeOptions, BasicAuthenticationHandler>(
            AuthenticationConstants.ApiKeySchemeName, _ => { });
    }

    private static void AddApiKeyPolicy(this AuthorizationBuilder authorizationBuilder)
    {
        authorizationBuilder.AddPolicy(AuthenticationConstants.ApiKeyPolicyName,
            policy => policy.AddAuthenticationSchemes(AuthenticationConstants.ApiKeySchemeName).RequireAuthenticatedUser());
    }
}