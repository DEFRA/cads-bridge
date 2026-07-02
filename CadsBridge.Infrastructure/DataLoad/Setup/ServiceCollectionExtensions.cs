using CadsBridge.Application.DataLoad.Messaging;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.Services;
using CadsBridge.Infrastructure.DataLoad.Messaging;
using CadsBridge.Infrastructure.DataLoad.Persistence;
using CadsBridge.Infrastructure.DataLoad.Services;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.DataLoad.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDataLoad(this IServiceCollection services)
    {
        services.RegisterCsvFileImporting();
        services.RegisterSqlDataSeeding();

        return services;
    }

    public static void RegisterCsvFileImporting(this IServiceCollection services)
    {
        services.AddHostedService<CsvDataFileImportBackgroundService>();
        services.AddHostedService<CsvDataFileSplitBackgroundService>();

        services.AddTransient<IS3CopyService, S3CopyService>();
        services.AddTransient<ICsvDataFileSplitterService, CsvDataFileSplitterService>();
        services.AddTransient<ICsvParser, CsvParser>();
        services.AddTransient<IS3FileMetaDataService, S3FileMetaDataService>();

        services.AddTransient<ISplitMessageProducer, SplitMessageProducer>();

        services.AddSingleton<IImportJobProgressStore, InMemoryImportJobProgressStore>();
        services.AddSingleton<ISplitJobProgressStore, InMemorySplitJobProgressStore>();
        services.AddSingleton<IFileImportStatusStore, FileImportStatusStore>();
    }

    public static void RegisterSqlDataSeeding(this IServiceCollection services)
    {
        services.AddHostedService<DataSeedImportBackgroundService>();

        services.AddTransient<IDataSeedFileLoadService, DataSeedFileLoadService>();
        services.AddTransient<IFileSystemToS3CopyService, FileSystemToS3CopyService>();
    }
}