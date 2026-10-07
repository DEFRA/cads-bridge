namespace CadsBridge.Testing.Support.Constants;

public static class TestAuthConstants
{
    public const string BasicApiKey = "TestClient";
    public const string BasicSecret = "test-secret";

    // ## cads test user
    public const string AzureAdCadsEmail = "test-viewer-user@internal.test";
    public const string AzureAdCadsUsername = "test-viewer-user";

    // ## Fakes: bearer token control values (consumed by FakeJwtHandler)
    public const string FakeJwtDefault = "fake-jwt-token";
    public const string FakeJwtMissingSqsAdminScope = "fake-jwt-token-missing-sqs-admin-scope";

    // ## Fakes: Azure AD
    public const string AzureAdFakeAuthority = "https://fake-issuer";

    // # Audience
    public const string AzureAdCadsCdsAudience = "api://local-cads-cds";
}