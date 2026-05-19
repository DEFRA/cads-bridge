using CadsBridge.Core.DataSeed.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.DataSeed.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDataSeed(this IServiceCollection services)
    {
        services.AddSingleton<IDataSeedFileLoader, DataSeedFileLoader>();

        return services;
    }
}