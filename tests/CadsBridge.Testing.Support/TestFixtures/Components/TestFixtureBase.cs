namespace CadsBridge.Testing.Support.TestFixtures.Components;

public abstract class TestFixtureBase<TStart, TFactory>
    where TStart : class
    where TFactory : WebAppFactoryBase<TStart>
{
    public readonly HttpClient HttpClient;
    public readonly TFactory Factory;

    protected TestFixtureBase(TFactory factory, bool useFakeAuth = false)
    {
        Factory = factory;
        HttpClient = factory.CreateClient();
    }
}