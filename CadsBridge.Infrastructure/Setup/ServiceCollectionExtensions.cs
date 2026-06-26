using CadsBridge.Core.FileSystem;
using CadsBridge.Infrastructure.ApiClients.Setup;
using CadsBridge.Infrastructure.Crypto;
using CadsBridge.Infrastructure.DataLoad.Setup;
using CadsBridge.Infrastructure.FileSystem;
using CadsBridge.Infrastructure.Storage.Setup;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddInfrastructureLayer(this IServiceCollection services, IConfiguration config, IHealthChecksBuilder healthChecksBuilder)
    {
        services.AddStorage(config);

        services.AddAmazonS3Core(config, healthChecksBuilder);

        services.AddApiClients(config, healthChecksBuilder);

        services.AddDataLoad();

        services.RegistryCrypto();

        services.RegistryFileSystem();

        return services;
    }

    public static void RegistryCrypto(this IServiceCollection services)
    {
        services.AddTransient<IAesCryptoTransform, AesCryptoTransform>();
    }

    public static void RegistryFileSystem(this IServiceCollection services)
    {
        services.AddTransient<IFileSystemWrapper, FileSystemWrapper>();
    }
}