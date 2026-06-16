using CadsBridge.Application.FileImport.Services;
using CadsBridge.Infrastructure.FileImport.Services;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.FileImport.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFileImport(this IServiceCollection services)
    {
        services.AddTransient<IS3ExternalToInternalCopyService, S3ExternalToInternalCopyService>();
        services.AddHostedService<FileImportBackgroundService>();

        return services;
    }
}