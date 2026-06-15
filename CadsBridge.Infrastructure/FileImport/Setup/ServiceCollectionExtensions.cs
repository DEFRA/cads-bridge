using CadsBridge.Application.FileImport.Services;
using CadsBridge.Application.Services;
using CadsBridge.Infrastructure.FileImport.Services;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.FileImport.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFileImport(this IServiceCollection services)
    {
        services.AddTransient<IS3ExternalToInternalCopyService, S3ExternalToInternalCopyService>();

        return services;
    }
}