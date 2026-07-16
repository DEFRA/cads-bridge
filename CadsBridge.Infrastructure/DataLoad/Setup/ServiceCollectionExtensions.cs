using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Application.DataLoad.Messaging;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using CadsBridge.Infrastructure.DataLoad.Csv.Factories;
using CadsBridge.Infrastructure.DataLoad.Csv.Services;
using CadsBridge.Infrastructure.DataLoad.Csv.Strategies;
using CadsBridge.Infrastructure.DataLoad.Messaging;
using CadsBridge.Infrastructure.DataLoad.Persistence;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Infrastructure.Storage.Clients;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.DataLoad.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDataLoad(this IServiceCollection services, IConfiguration configuration)
    {
        var dataLoadConfig = configuration
            .GetSection("DataLoad")
            .Get<DataLoadConfiguration>()
            ?? throw new InvalidOperationException("Missing 'DataLoad' config");
        services.AddSingleton(dataLoadConfig);

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
        services.AddTransient<ICsvDataFileSplitterStrategyFactory, CsvDataFileSplitterFactory>();
        services.AddTransient<ICsvDataFileSplitterStrategy, CsvDataFileSplitterStrategyNone>();
        services.AddTransient<ICsvDataFileSplitterStrategy, CsvDataFileSplitterStrategyByLines>();
        services.AddTransient<ICsvDataFileSplitterStrategy, CsvDataFileSplitterStrategyBySize>();
        services.AddTransient<ICsvParser, CsvParser>();
        services.AddTransient<IS3FileMetaDataService, S3FileMetaDataService<InternalStorageClient>>();
        services.AddTransient<ISplitMessageProducer, SplitMessageProducer>();
        services.AddTransient<IFileImportStore, FileImportStore>();
    }

    public static void RegisterSqlDataSeeding(this IServiceCollection services)
    {
        services.AddHostedService<DataSeedImportBackgroundService>();

        services.AddTransient<IDataSeedFileLoadService, DataSeedFileLoadService>();
        services.AddTransient<IFileSystemToS3CopyService, FileSystemToS3CopyService>();
    }
}