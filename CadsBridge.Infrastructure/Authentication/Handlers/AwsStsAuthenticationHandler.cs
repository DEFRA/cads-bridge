using System.Net.Http.Headers;
using CadsBridge.Infrastructure.Authentication.Providers;

namespace CadsBridge.Infrastructure.Authentication.Handlers;

public class AwsStsAuthenticationHandler(IAwsStsProvider awsStsProvider, string audience) : DelegatingHandler
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var credentials = await awsStsProvider.GetCredentialsAsync(audience, cancellationToken);

        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", credentials.Token);
        return await base.SendAsync(request, cancellationToken);
    }
}