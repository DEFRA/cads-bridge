using CadsBridge.Testing.Support.TestFixtures.Components;

namespace CadsBridge.Tests.Component.TestFixtures;

public class CadsBridgeTestFixture(CadsBridgeWebAppFactory factory)
    : TestFixtureBase<Program, CadsBridgeWebAppFactory>(factory);