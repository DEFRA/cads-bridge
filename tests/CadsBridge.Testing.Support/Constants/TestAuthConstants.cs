namespace CadsBridge.Testing.Support.Constants;

public static class TestAuthConstants
{
    // # Basic ApiKey
    public const string BasicApiKey = "TestClient";
    public const string BasicSecret = "test-secret";

    // # Scopes
    public const string AzureAdAdminQueueManagerScope = "admin.queue.manager";

    // Mapped to the same API resource/audience as the admin scope, but intentionally not
    // granted any authorization policy - lets tests request a token with a valid "aud"
    // claim while still lacking the scope required by [Authorize] policies.
    public const string AzureAdNoOpScope = "noop.scope";

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

    // ### TestUser (GrantType: password)
    public const string AzureAdTestUserClientId = "local-cads-test-user-client";
    public const string AzureAdTestUserClient = "local-mock-client";

    // # Users
    public const string AzureAdPassword = "password";

    // ## cads-admin-frontend test user
    public const string AzureAdCadsAdminEmail = "cads-admin-user@internal.test";
    public const string AzureAdCadsAdminUsername = "cads-admin-user";
}