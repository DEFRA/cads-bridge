using CadsBridge.Infrastructure.Authentication.Configuration;
using CadsBridge.Setup;
using FluentAssertions;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace CadsBridge.Tests.Unit.Authentication;

public class ConfigureAuthenticationTests
{
    [Fact]
    public async Task ConfigureAuthentication_AddsAwsStsSchemeToDefaultPolicy_WhenEnabled()
    {
        var provider = BuildProvider(apiKeyEnabled: false, awsStsEnabled: true);

        var scheme = await provider.GetRequiredService<IAuthenticationSchemeProvider>()
            .GetSchemeAsync(AuthenticationConstants.AwsStsSchemeName);
        var policy = await provider.GetRequiredService<IAuthorizationPolicyProvider>()
            .GetPolicyAsync(AuthenticationConstants.DefaultPolicy);

        scheme.Should().NotBeNull();
        provider.GetRequiredService<IOptionsMonitor<AuthenticationSchemeOptions>>()
            .Get(AuthenticationConstants.AwsStsSchemeName).Should().NotBeNull();
        policy!.AuthenticationSchemes.Should().Contain(AuthenticationConstants.AwsStsSchemeName);
    }

    [Fact]
    public async Task ConfigureAuthentication_DoesNotAddAwsStsScheme_WhenDisabled()
    {
        var provider = BuildProvider(apiKeyEnabled: true, awsStsEnabled: false);

        var scheme = await provider.GetRequiredService<IAuthenticationSchemeProvider>()
            .GetSchemeAsync(AuthenticationConstants.AwsStsSchemeName);
        var policy = await provider.GetRequiredService<IAuthorizationPolicyProvider>()
            .GetPolicyAsync(AuthenticationConstants.DefaultPolicy);

        scheme.Should().BeNull();
        policy!.AuthenticationSchemes.Should().NotContain(AuthenticationConstants.AwsStsSchemeName);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ConfigureAuthentication_ScopePolicies_IncludeAwsSts_OnlyWhenEnabled(bool awsStsEnabled)
    {
        var provider = BuildProvider(apiKeyEnabled: true, awsStsEnabled: awsStsEnabled);

        var policy = await provider.GetRequiredService<IAuthorizationPolicyProvider>()
            .GetPolicyAsync(AuthenticationConstants.AadSqsAdminExecutePolicy);

        policy!.AuthenticationSchemes.Contains(AuthenticationConstants.AwsStsSchemeName)
            .Should().Be(awsStsEnabled);
    }

    private static ServiceProvider BuildProvider(bool apiKeyEnabled, bool awsStsEnabled)
    {
        var configDict = new Dictionary<string, string?>
        {
            ["AuthenticationConfiguration:ApiKey:Enabled"] = apiKeyEnabled.ToString(),
            ["AuthenticationConfiguration:AwsSts:Enabled"] = awsStsEnabled.ToString(),
            ["AuthenticationConfiguration:AzureAD:Enabled"] = "false"
        };

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(configDict)
            .Build();

        var services = new ServiceCollection();
        services.ConfigureAuthentication(config);

        return services.BuildServiceProvider();
    }
}