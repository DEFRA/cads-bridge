using CadsBridge.Infrastructure.Authentication.DTOs;

namespace CadsBridge.Infrastructure.Authentication.Providers;

public interface IAwsStsProvider
{
    Task<IdentityToken> GetCredentialsAsync(string audience, CancellationToken cancellationToken = default);
}