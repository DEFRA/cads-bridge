using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Testing.Support.TestFixtures.Components;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;

namespace CadsBridge.Tests.Component.TestFixtures;

public class CadsBridgeWebAppFactory(
    IDictionary<string, string?>? configOverrides = null,
    bool disableHostedServices = true)
    : WebAppFactoryBase<Program>(
        configOverrides,
        disableHostedServices)
{
    public CadsBridgeWebAppFactory() : this(null) { }

    public Mock<IDataSeedFileLoadService> DataSeedFileLoaderMock { get; } = new();
    public Mock<IS3FileMetaDataService> S3FileMetaDataServiceMock { get; } = CreateDefaultS3FileMetaDataServiceMock();
    public Mock<IFileImportStore> FileImportStoreMock { get; } = CreateDefaultFileImportStoreMock();

    private readonly List<Action<IServiceCollection>> _testServiceOverrides = [];

    private static Mock<IS3FileMetaDataService> CreateDefaultS3FileMetaDataServiceMock()
    {
        var mock = new Mock<IS3FileMetaDataService>();
        mock.Setup(x => x.GetRecordCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(0L);
        return mock;
    }

    private static Mock<IFileImportStore> CreateDefaultFileImportStoreMock()
    {
        var mock = new Mock<IFileImportStore>();
        mock.Setup(x => x.CreateAsync(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(1L);
        mock.Setup(x => x.MarkTransferredAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        mock.Setup(x => x.MarkSplitAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        mock.Setup(x => x.MarkCompletedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        mock.Setup(x => x.MarkFailedAsync(It.IsAny<long>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        return mock;
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        base.ConfigureWebHost(builder);

        builder.ConfigureTestServices(services =>
        {
            services.RemoveAll<IDataSeedFileLoadService>();
            services.AddSingleton(DataSeedFileLoaderMock.Object);

            OverrideFileImportStatusStore(services);

            foreach (var overrideAction in _testServiceOverrides)
            {
                overrideAction(services);
            }
        });
    }

    public void OverrideHttpClientHandler(string clientName, HttpMessageHandler handler)
    {
        _testServiceOverrides.Add(services =>
            services.AddHttpClient(clientName)
                .ConfigurePrimaryHttpMessageHandler(() => handler));
    }

    private void OverrideFileImportStatusStore(IServiceCollection services)
    {
        services.RemoveAll<IS3FileMetaDataService>();
        services.AddSingleton(S3FileMetaDataServiceMock.Object);
        services.RemoveAll<IFileImportStore>();
        services.AddSingleton(FileImportStoreMock.Object);
    }
}