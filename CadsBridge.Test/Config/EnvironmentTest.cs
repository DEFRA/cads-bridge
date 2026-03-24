using Microsoft.AspNetCore.Builder;

namespace CadsBridge.Test.Config;

public class EnvironmentTest
{
    [Fact]
    public void IsNotDevModeByDefault()
    {
        var builder = WebApplication.CreateEmptyBuilder(new WebApplicationOptions());
        var isDev = CadsBridge.Config.Environment.IsDevMode(builder);
        Assert.False(isDev);
    }
}