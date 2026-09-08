using System.Net.Http.Headers;
using System.Text;
using CadsBridge.Infrastructure.Authentication.Configuration;
using CadsBridge.Testing.Support.Constants;

namespace CadsBridge.Testing.Support.Utilities.Http;

public static class HttpClientExtensions
{
    extension(HttpClient client)
    {
        public HttpClient AddBasicTestApiKey()
        {
            var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{TestAuthConstants.BasicApiKey}:{TestAuthConstants.BasicSecret}"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(AuthenticationConstants.ApiKeySchemeName, credentials);
            return client;
        }
    }
}