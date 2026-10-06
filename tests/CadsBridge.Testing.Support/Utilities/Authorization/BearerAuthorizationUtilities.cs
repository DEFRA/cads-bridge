using System.Net.Http.Headers;
using CadsBridge.Testing.Support.Constants;

namespace CadsBridge.Testing.Support.Utilities.Authorization;

public static class BearerAuthorizationUtilities
{
    public static void AddJwt(this HttpClient client)
    {
        client.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", TestAuthConstants.FakeJwtDefault);
    }

    public static void AddJwt(this HttpClient client, string token)
    {
        client.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", token);
    }
}