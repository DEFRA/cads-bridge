using CadsBridge.Testing.Support.TestFixtures.Components;

namespace CadsBridge.Tests.Component.TestFixtures;

public class CadsBridgeTestFixture
    : TestFixtureBase<Program, CadsBridgeWebAppFactory>, IAsyncDisposable
{
    public CadsBridgeTestFixture() : base(new CadsBridgeWebAppFactory(useFakeAuth: true), useFakeAuth: true)
    {
    }

    public async ValueTask DisposeAsync()
    {
        HttpClient.Dispose();
        await Factory.DisposeAsync();
    }
}