using CadsBridge.Infrastructure.Authentication.Configuration;
using CadsBridge.Testing.Support.Constants;

namespace CadsBridge.Testing.Support.TestFixtures.Containers.Configuration;

public class TestAwsStsConfiguration : AwsStsConfiguration
{
    public TestAwsStsConfiguration(StsMockFixture sts)
    {
        Enabled = true;
        Audience = TestAuthConstants.StsAudience;
        TrustedOrgId = TestAuthConstants.StsTrustedOrgId;
        Environment = TestAuthConstants.StsEnvironment;
        RequireHttpsMetadata = false;

        // Container-facing issuer: the API fetches discovery from inside Docker
        TrustedIssuers = [sts.Issuer];
    }
}