using CadsBridge.Application.FileSplit.Services;
using CadsBridge.Infrastructure.FileSplit.Services;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.FileSplit.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFileSplit(this IServiceCollection services)
    {
        services.AddHostedService<FileSplitBackgroundService>();
        services.AddTransient<IS3FileSplitterService, S3FileSplitterService>();
        services.AddTransient<ISplitMessageProducer, SplitMessageProducer>();

        return services;
    }
}