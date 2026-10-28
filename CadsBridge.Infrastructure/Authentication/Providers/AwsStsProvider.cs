using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using CadsBridge.Infrastructure.Authentication.DTOs;

namespace CadsBridge.Infrastructure.Authentication.Providers;

public sealed class AwsStsProvider(IAmazonSecurityTokenService stsClient, TimeProvider timeProvider) : IAwsStsProvider
{
    private const int DurationSeconds = 300;
    private const int RefreshMarginSeconds = 30;
    private const string SigningAlgorithm = "RS256";

    private readonly ConcurrentDictionary<string, IdentityToken> _cache = new();
    private readonly SemaphoreSlim _lock = new(1, 1);

    public async Task<IdentityToken> GetCredentialsAsync(string audience, CancellationToken cancellationToken = default)
    {
        if (IsStillValid(audience, out var cached))
            return cached;

        await _lock.WaitAsync(cancellationToken);
        try
        {
            if (IsStillValid(audience, out cached))
                return cached;

            var response = await stsClient.GetWebIdentityTokenAsync(new GetWebIdentityTokenRequest { Audience = [audience], SigningAlgorithm = SigningAlgorithm, DurationSeconds = DurationSeconds }, cancellationToken);
            var expiration = response.Expiration ?? timeProvider.GetUtcNow();

            var identityToken = new IdentityToken(response.WebIdentityToken, expiration);
            _cache[audience] = identityToken;

            return identityToken;
        }
        finally
        {
            _lock.Release();
        }
    }

    private bool IsStillValid(string audience, [NotNullWhen(true)]out IdentityToken? identityToken)
    {
        if (_cache.TryGetValue(audience, out var cached) &&
            cached.Expiration - TimeSpan.FromSeconds(RefreshMarginSeconds) > timeProvider.GetUtcNow())
        {
            identityToken = cached;
            return true;
        }

        identityToken = null;
        return false;
    }
}