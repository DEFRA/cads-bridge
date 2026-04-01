using CadsBridge.Application.Setup;
using CadsBridge.Infrastructure.Configuration.Aws;
using CadsBridge.Infrastructure.Json;
using CadsBridge.Infrastructure.Setup;

namespace CadsBridge.Setup;

public static class ServiceCollectionExtensions
{
    public static void ConfigureCds(this IServiceCollection services, IConfiguration configuration)
    {
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
        services.AddApplicationLayer(configuration);

        services.ConfigureHealthChecks();
    }

    private static void ConfigureHealthChecks(this IServiceCollection services)
    {
        var builder = services.AddHealthChecks();
    }
}