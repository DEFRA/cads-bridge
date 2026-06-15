using CadsBridge.Application.DataSeed.Services;
using CadsBridge.Application.Services;
using CadsBridge.Core.DataSeed.Abstractions;
using CadsBridge.Infrastructure.DataSeed.Services;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.DataSeed.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDataSeed(this IServiceCollection services)
    {
        services.AddSingleton<IDataSeedFileLoader, DataSeedFileLoader>();
        services.AddTransient<IFileSystemToS3CopyService, FileSystemToS3CopyService>();

        return services;
    }
}