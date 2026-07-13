using CadsBridge.Tests.Component.TestFixtures;

namespace CadsBridge.Tests.Component.Consumers;

public class CadsBridgeFifoQueuePollerTests(CadsBridgeTestFixture appTestFixture) : IClassFixture<CadsBridgeTestFixture>
{
    private readonly CadsBridgeTestFixture _appTestFixture = appTestFixture;


}
