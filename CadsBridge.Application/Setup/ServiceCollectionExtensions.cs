using CadsBridge.Application.Models;
using CadsBridge.Application.Persistance;
using CadsBridge.Application.Services;
using CadsBridge.Core.Crypto;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Channels;
using CadsBridge.Core.Storage.FileSystem;

namespace CadsBridge.Application.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddApplicationLayer(this IServiceCollection services, IConfiguration config)
    {
        services.AddSingleton<Channel<FileImportJob>>(Channel.CreateUnbounded<FileImportJob>(new UnboundedChannelOptions() { SingleReader = false }));
        services.AddSingleton<Channel<FileSplitJob>>(Channel.CreateUnbounded<FileSplitJob>(new UnboundedChannelOptions() { SingleReader = false }));
        services.AddSingleton<Channel<DataSeedImportJob>>(Channel.CreateUnbounded<DataSeedImportJob>(new UnboundedChannelOptions() { SingleReader = false }));
        services.AddSingleton<IImportJobProgressStore, InMemoryImportJobProgressStore>();
        services.AddSingleton<ISplitJobProgressStore, InMemorySplitJobProgressStore>();
        services.AddSingleton<IFileSytemWrapper, FileSystemWrapper>();

        services.AddTransient<IAesCryptoTransform, AesCryptoTransform>();
        services.AddTransient<ISplitMessageProducer, SplitMessageProducer>();
        services.AddTransient<IS3FileSplitterService, S3FileSplitterService>();
        services.AddTransient<IDataSeedFileCopyService, DataSeedFileCopyService>();
        services.AddTransient<IFileImportCopyService, FileImportCopyService>();
        services.AddTransient<IAmazonTransferServiceWrapper, AmazonTransferServiceWrapper>();

        services.AddHostedService<FileImportBackgroundService>();
        services.AddHostedService<FileSplitBackgroundService>();
        services.AddHostedService<DataSeedImportBackgroundService>();

        return services;
    }
}