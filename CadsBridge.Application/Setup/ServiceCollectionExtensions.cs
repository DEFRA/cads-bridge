using CadsBridge.Application.DataLoad.Jobs;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Channels;

namespace CadsBridge.Application.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddApplicationLayer(this IServiceCollection services)
    {
        services.ConfigureMediatR();
        services.RegisterChannels();

        return services;
    }

    private static void ConfigureMediatR(this IServiceCollection services)
    {
        services.AddMediatR(cfg =>
        {
            cfg.RegisterServicesFromAssembly(typeof(IApplicationMarker).Assembly);
        });
    }

    public static void RegisterChannels(this IServiceCollection services)
    {
        services.AddSingleton(Channel.CreateUnbounded<CsvDataFileImportJob>(new UnboundedChannelOptions { SingleReader = false }));
        services.AddSingleton(Channel.CreateUnbounded<CsvDataFileSplitJob>(new UnboundedChannelOptions { SingleReader = false }));
        services.AddSingleton(Channel.CreateUnbounded<DataSeedFileLoadJob>(new UnboundedChannelOptions { SingleReader = false }));
    }
}