using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Amazon;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using CadsBridge.Infrastructure.ApiClients.Configuration;

namespace CadsBridge.Infrastructure.Authentication.Fakes;

public sealed class MockSecurityTokenServiceClient(
    IHttpClientFactory httpClientFactory,
    StsMockConfig mockConfig,
    TimeProvider timeProvider)
    : AmazonSecurityTokenServiceClient(new AnonymousAWSCredentials(), RegionEndpoint.EUWest2)
{
    public const string HttpClientName = "StsMock";

    private readonly string _tokenEndpoint = mockConfig.TokenEndpoint;

    public override async Task<GetWebIdentityTokenResponse> GetWebIdentityTokenAsync(
        GetWebIdentityTokenRequest request,
        CancellationToken cancellationToken = default)
    {
        using var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = mockConfig.ClientId,
            ["client_secret"] = mockConfig.ClientSecret,
            ["scope"] = mockConfig.Scope
        });

        using var client = httpClientFactory.CreateClient(HttpClientName);

        HttpResponseMessage response;
        try
        {
            response = await client.PostAsync(_tokenEndpoint, content, cancellationToken);
        }
        catch (HttpRequestException ex)
        {
            throw new AmazonClientException($"STS mock at '{_tokenEndpoint}' could not be reached.", ex);
        }

        using (response)
        {
            if (!response.IsSuccessStatusCode)
            {
                var errorType = (int)response.StatusCode >= 500 ? ErrorType.Receiver : ErrorType.Sender;
                throw new AmazonServiceException(
                    $"STS mock at '{_tokenEndpoint}' returned {(int)response.StatusCode}.",
                    errorType,
                    response.StatusCode.ToString(),
                    requestId: string.Empty,
                    response.StatusCode);
            }

            var token = await response.Content.ReadFromJsonAsync<TokenResponse>(cancellationToken)
                ?? throw new AmazonClientException($"STS mock at '{_tokenEndpoint}' returned an empty token response.");

            return new GetWebIdentityTokenResponse
            {
                WebIdentityToken = token.AccessToken,
                Expiration = timeProvider.GetUtcNow().UtcDateTime.AddSeconds(token.ExpiresIn),
                HttpStatusCode = HttpStatusCode.OK
            };
        }
    }

    private sealed record TokenResponse(
        [property: JsonPropertyName("access_token")] string AccessToken,
        [property: JsonPropertyName("expires_in")] int ExpiresIn);
}