namespace CadsBridge.Testing.Support.TestFixtures.Components;

public abstract class TestFixtureBase<TStart, TFactory>(TFactory factory)
    where TStart : class
    where TFactory : WebAppFactoryBase<TStart>
{
    public readonly HttpClient HttpClient = factory.CreateClient();
    public readonly TFactory Factory = factory;
}