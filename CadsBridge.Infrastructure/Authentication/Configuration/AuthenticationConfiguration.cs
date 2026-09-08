namespace CadsBridge.Infrastructure.Authentication.Configuration;

public class AuthenticationConfiguration
{
    public AuthenticationStateConfiguration ApiKey { get; set; } = new();
}

public class AuthenticationStateConfiguration
{
    public bool Enabled { get; set; }
}

public static class AuthenticationConstants
{
    public const string ApiKeySchemeName = "Basic";
    public const string ApiKeyPolicyName = "ApiKey";
}