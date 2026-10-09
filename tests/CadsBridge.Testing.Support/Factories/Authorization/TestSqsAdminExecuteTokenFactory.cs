using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.Fakes.Authentication;
using CadsBridge.Testing.Support.Utilities.Authorization;

namespace CadsBridge.Testing.Support.Factories.Authorization;

public static class TestSqsAdminExecuteTokenFactory
{
    public static TestTokenRequest ValidUserToken() =>
        TestTokenFactory.UserToken(
            TestAuthConstants.AzureAdCadsAdminUsername,
            TestAuthConstants.AzureAdPassword,
            TestAuthConstants.AzureAdAdminQueueManagerScope);

    public static TestTokenRequest MissingScopeToken() =>
        TestTokenFactory.UserToken(
            TestAuthConstants.AzureAdCadsAdminUsername,
            TestAuthConstants.AzureAdPassword,
            TestAuthConstants.AzureAdNoOpScope);   // valid audience, but no admin.queue.manager
}