using CadsBridge.Infrastructure.DataSeed.Setup;
using CadsBridge.Infrastructure.FileImport.Setup;
using CadsBridge.Infrastructure.Storage.Setup;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddInfrastructureLayer(this IServiceCollection services, IConfiguration config)
    {
        services.AddDataSeed();
        services.AddFileImport();
        services.AddStorage(config);
        services.AddAmazonS3Core(config);

        return services;
    }
}