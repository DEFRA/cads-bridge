using CadsBridge.Application.DataLoad.Jobs;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Channels;

namespace CadsBridge.Application.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddApplicationLayer(this IServiceCollection services)
    {
        services.RegisterChannels();

        return services;
    }

    public static void RegisterChannels(this IServiceCollection services)
    {
        services.AddSingleton<Channel<CsvDataFileImportJob>>(Channel.CreateUnbounded<CsvDataFileImportJob>(new UnboundedChannelOptions() { SingleReader = false }));
        services.AddSingleton<Channel<CsvDataFileSplitJob>>(Channel.CreateUnbounded<CsvDataFileSplitJob>(new UnboundedChannelOptions() { SingleReader = false }));
        services.AddSingleton<Channel<DataSeedFileLoadJob>>(Channel.CreateUnbounded<DataSeedFileLoadJob>(new UnboundedChannelOptions() { SingleReader = false }));
    }
}