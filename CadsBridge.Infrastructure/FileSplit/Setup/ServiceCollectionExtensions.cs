using CadsBridge.Application.FileSplit.Services;
using CadsBridge.Infrastructure.FileSplit.Services;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.FileSplit.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFileSplit(this IServiceCollection services)
    {
        services.AddHostedService<HistoricDataCsvFileSplitBackgroundService>();
        services.AddTransient<Application.FileSplit.Services.IS3HistoricDataFileSplitterService, S3HistoricDataFileSplitterService>();
        services.AddTransient<ISplitMessageProducer, SplitMessageProducer>();

        return services;
    }
}