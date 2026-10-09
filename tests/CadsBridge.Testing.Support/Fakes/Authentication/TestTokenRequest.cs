namespace CadsBridge.Testing.Support.Fakes.Authentication;

public sealed class TestTokenRequest
{
    public required string ClientId { get; init; }
    public required string ClientSecret { get; init; }

    // Username null => client_credentials
    public string? Username { get; init; }
    public string? Password { get; init; }

    public required string[] Scopes { get; init; }
}