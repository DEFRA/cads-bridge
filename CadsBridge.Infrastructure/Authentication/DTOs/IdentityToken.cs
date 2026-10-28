namespace CadsBridge.Infrastructure.Authentication.DTOs;

public record IdentityToken(string Token, DateTimeOffset Expiration);