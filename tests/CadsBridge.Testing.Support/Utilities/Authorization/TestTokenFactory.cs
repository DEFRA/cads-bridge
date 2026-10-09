using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.Fakes.Authentication;

namespace CadsBridge.Testing.Support.Utilities.Authorization;

public static class TestTokenFactory
{
    private static readonly string[] s_baseScopes = ["openid", "profile", "email"];

    public static TestTokenRequest UserToken(
        string username,
        string password,
        params string[] scopes) =>
        new()
        {
            ClientId = TestAuthConstants.AzureAdTestUserClientId,
            ClientSecret = TestAuthConstants.AzureAdTestUserClient,
            Username = username,
            Password = password,
            Scopes = [.. s_baseScopes, .. scopes]
        };
}