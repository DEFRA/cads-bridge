using CadsBridge.Application.Models;
using CadsBridge.Application.Persistence;
using CadsBridge.Core.Crypto;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Channels;

namespace CadsBridge.Application.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddApplicationLayer(this IServiceCollection services, IConfiguration config)
    {
        services.AddSingleton<Channel<ImportHistoricCattleFileJob>>(Channel.CreateUnbounded<ImportHistoricCattleFileJob>(new UnboundedChannelOptions() { SingleReader = false }));
        services.AddSingleton<Channel<HistoricDataFileSplitJob>>(Channel.CreateUnbounded<HistoricDataFileSplitJob>(new UnboundedChannelOptions() { SingleReader = false }));
        services.AddSingleton<Channel<DataSeedImportJob>>(Channel.CreateUnbounded<DataSeedImportJob>(new UnboundedChannelOptions() { SingleReader = false }));
        services.AddSingleton<IImportJobProgressStore, InMemoryImportJobProgressStore>();
        services.AddSingleton<ISplitJobProgressStore, InMemorySplitJobProgressStore>();

        services.AddTransient<IAesCryptoTransform, AesCryptoTransform>();

        return services;
    }
}