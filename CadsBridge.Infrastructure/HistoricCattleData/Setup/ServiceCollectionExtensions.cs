using CadsBridge.Application.HistoricCattleData.Services;
using CadsBridge.Infrastructure.HistoricCattleData.Services;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.FileImport.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFileImport(this IServiceCollection services)
    {
        services.AddTransient<IS3ExternalToInternalCopyService, S3ExternalToInternalCopyService>();
        services.AddHostedService<BackfillHistoricCattleDataBackgroundService>();

        return services;
    }
}