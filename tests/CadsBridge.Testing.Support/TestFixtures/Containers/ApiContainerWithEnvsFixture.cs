namespace CadsBridge.Testing.Support.TestFixtures.Containers;

public class ApiContainerWithEnvsFixture(IDictionary<string, string>? extraEnvironment)
    : ApiContainerFixtureBase(extraEnvironment)
{
}