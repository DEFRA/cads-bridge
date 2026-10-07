namespace CadsBridge.Infrastructure.Authentication.Configuration;

public class AuthenticationConfiguration
{
    public AuthenticationStateConfiguration ApiKey { get; set; } = new();
    public AuthenticationProviderConfiguration AzureAD { get; set; } = new();
}

public class AuthenticationStateConfiguration
{
    public bool Enabled { get; set; }
}

public class AuthenticationProviderConfiguration : AuthenticationStateConfiguration
{
    public string? Authority { get; set; }
    public string? Audience { get; set; }
    public string? MetadataAddress { get; set; }
    public bool RequireHttpsMetadata { get; set; } = true;
    public bool ValidateIssuer { get; set; } = true;
    public string ScopeClaimType { get; set; } = "http://schemas.microsoft.com/identity/claims/scope";
    public string RoleClaimType { get; set; } = "http://schemas.microsoft.com/ws/2008/06/identity/claims/role";
}

public static class AuthenticationConstants
{
    public const string ApiKeySchemeName = "Basic";
    public const string AzureADSchemeName = "AzureAd";


    // Policy names
    public const string ApiKeyPolicyName = "ApiKey";
    public const string AadSqsAdminExecutePolicy = "AadSqsAdminExecute";
}