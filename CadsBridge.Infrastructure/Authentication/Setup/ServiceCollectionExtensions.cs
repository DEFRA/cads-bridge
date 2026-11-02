using Amazon.SecurityToken;
using CadsBridge.Infrastructure.Authentication.Fakes;
using CadsBridge.Infrastructure.Authentication.Providers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.Authentication.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddAwsSts(this IServiceCollection services, IConfiguration configuration)
    {
        var stsMockConfig = configuration.GetSection("ApiClients:CdsApi:Sts:Mock").Get<StsMockConfig>() ?? new StsMockConfig();
        if (stsMockConfig.Enabled)
        {
            services.AddHttpClient(MockSecurityTokenServiceClient.HttpClientName);
            services.AddSingleton<IAmazonSecurityTokenService>(sp => new MockSecurityTokenServiceClient(
                sp.GetRequiredService<IHttpClientFactory>(),
                stsMockConfig,
                sp.GetRequiredService<TimeProvider>()));
        }
        else
        {
            services.AddAWSService<IAmazonSecurityTokenService>();
        }
        services.AddSingleton<IAwsStsProvider, AwsStsProvider>();

        return services;
    }
}