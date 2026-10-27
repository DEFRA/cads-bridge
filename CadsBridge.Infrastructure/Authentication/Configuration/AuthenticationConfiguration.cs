namespace CadsBridge.Infrastructure.Authentication.Configuration;

public class AuthenticationConfiguration
{
    public AuthenticationStateConfiguration ApiKey { get; set; } = new();
    public AwsStsConfiguration AwsSts { get; set; } = new();
    public AuthenticationProviderConfiguration AzureAD { get; set; } = new();
}

public class AuthenticationStateConfiguration
{
    public bool Enabled { get; set; }
}

public class AwsStsConfiguration : AuthenticationStateConfiguration
{
    public string Audience { get; set; } = string.Empty;
    public string TrustedOrgId { get; set; } = string.Empty;
    public string Environment { get; set; } = string.Empty;
    public bool RequireHttpsMetadata { get; set; } = true;

    // Exact "iss" values we accept. Discovery and JWKS are fetched from the token's own issuer,
    // so without this allow list anyone hosting their own JWKS could mint an accepted token.
    public string[] TrustedIssuers { get; set; } = [];
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
    public const string AwsStsSchemeName = "AwsSts";
    public const string AzureADSchemeName = "AzureAd";

    // # Policy names
    public const string DefaultPolicy = "Default";

    // ## Custom policies
    public const string AadSqsAdminExecutePolicy = "AadSqsAdminExecute";
}