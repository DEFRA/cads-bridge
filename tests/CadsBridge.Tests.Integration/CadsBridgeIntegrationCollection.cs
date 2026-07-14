using CadsBridge.Testing.Support.TestFixtures.Containers;

namespace CadsBridge.Tests.Integration;

[CollectionDefinition("CadsBridgeIntegration")]
public class CadsBridgeIntegrationCollection :
ICollectionFixture<ApiContainerFixture>
{
}
