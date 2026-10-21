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
    public string ScopeClaimType { get; set; } = string.Empty;
    public string RoleClaimType { get; set; } = string.Empty;
}

public static class AuthenticationConstants
{
    // # Scheme names
    public const string ApiKeySchemeName = "Basic";
    public const string AzureADSchemeName = "AzureAd";

    // # Policy names
    public const string DefaultPolicy = "Default";

    // ## Custom policies
    public const string AadSqsAdminExecutePolicy = "AadSqsAdminExecute";
}