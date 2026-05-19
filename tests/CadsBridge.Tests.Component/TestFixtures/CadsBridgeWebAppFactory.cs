using CadsBridge.Core.DataSeed.Abstractions;
using CadsBridge.Testing.Support.TestFixtures.Components;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;

namespace CadsBridge.Tests.Component.Fixtures;

public class CadsBridgeWebAppFactory(IDictionary<string, string?>? configOverrides = null)
    : WebAppFactoryBase<Program>(configOverrides)
{
    public CadsBridgeWebAppFactory() : this(null) { }

    public Mock<IDataSeedFileLoader> DataSeedFileLoaderMock { get; } = new();

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        base.ConfigureWebHost(builder);

        builder.ConfigureTestServices(services =>
        {
            services.RemoveAll<IDataSeedFileLoader>();
            services.AddSingleton(DataSeedFileLoaderMock.Object);
        });
    }
}