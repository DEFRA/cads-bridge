using CadsBridge.Testing.Support.TestFixtures.Components;

namespace CadsBridge.Tests.Component.Fixtures;

public class CadsBridgeTestFixture(CadsBridgeWebAppFactory factory)
    : TestFixtureBase<Program, CadsBridgeWebAppFactory>(factory);