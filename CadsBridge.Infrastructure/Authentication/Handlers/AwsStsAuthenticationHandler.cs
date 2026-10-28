using System.Net;
using System.Net.Http.Headers;
using Amazon.Runtime;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Authentication.DTOs;
using CadsBridge.Infrastructure.Authentication.Providers;

namespace CadsBridge.Infrastructure.Authentication.Handlers;

public class AwsStsAuthenticationHandler(IAwsStsProvider awsStsProvider, string audience) : DelegatingHandler
{
    private static readonly HashSet<string> s_throttlingErrorCodes = ["Throttling", "ThrottlingException", "RequestLimitExceeded", "TooManyRequestsException"];

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var credentials = await GetCredentialsAsync(cancellationToken);

        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", credentials.Token);
        return await base.SendAsync(request, cancellationToken);
    }

    private async Task<IdentityToken> GetCredentialsAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await awsStsProvider.GetCredentialsAsync(audience, cancellationToken);
        }
        catch (AmazonServiceException ex) when (IsTransient(ex))
        {
            throw new RetryableException($"Transient failure retrieving STS token for audience '{audience}'. Status {(int)ex.StatusCode} ({ex.ErrorCode}).", ex);
        }
        catch (AmazonServiceException ex)
        {
            throw new NonRetryableException($"STS rejected token request for audience '{audience}'. Status {(int)ex.StatusCode} ({ex.ErrorCode}).", ex);
        }
        catch (AmazonClientException ex)
        {
            throw new RetryableException($"Failed to retrieve STS token for audience '{audience}'.", ex);
        }
    }

    private static bool IsTransient(AmazonServiceException ex) =>
        ex.StatusCode is HttpStatusCode.TooManyRequests or >= HttpStatusCode.InternalServerError
        || (ex.ErrorCode is not null && s_throttlingErrorCodes.Contains(ex.ErrorCode));
}
