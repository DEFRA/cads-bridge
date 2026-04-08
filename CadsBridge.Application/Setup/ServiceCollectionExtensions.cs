using CadsBridge.Application.Models;
using CadsBridge.Application.Persistance;
using CadsBridge.Application.Services;
using CadsBridge.Infrastructure.Crypto;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Factories;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Channels;

namespace CadsBridge.Application.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddApplicationLayer(this IServiceCollection services, IConfiguration config)
    {
        services.AddSingleton<Channel<FileImportJob>>(Channel.CreateUnbounded<FileImportJob>(new UnboundedChannelOptions() { SingleReader = false }));
        services.AddSingleton<Channel<FileSplitJob>>(Channel.CreateUnbounded<FileSplitJob>(new UnboundedChannelOptions() { SingleReader = false }));
        services.AddSingleton<ISplitMessageProducer, SplitMessageProducer>();

        services.AddSingleton<IImportJobProgressStore, InMemoryImportJobProgressStore>();
        services.AddSingleton<ISplitJobProgressStore, InMemorySplitJobProgressStore>();
        services.AddSingleton<IS3ClientFactory, S3ClientFactory>();
        services.AddTransient<IAesCryptoTransform, AesCryptoTransform>();
        services.AddHostedService<FileImportBackgroundService>();

        return services;
    }
}