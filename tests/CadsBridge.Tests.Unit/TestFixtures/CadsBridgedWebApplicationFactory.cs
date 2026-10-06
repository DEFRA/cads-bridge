using CadsBridge.Testing.Support.TestFixtures.Components;

namespace CadsBridge.Tests.Unit.TestFixtures;

public class CadsBridgedWebApplicationFactory(
    IDictionary<string, string?>? configOverrides = null, bool useFakeAuth = false)
    : WebAppFactoryBase<Program>(
        configOverrides: configOverrides, useFakeAuth: useFakeAuth)
{
}