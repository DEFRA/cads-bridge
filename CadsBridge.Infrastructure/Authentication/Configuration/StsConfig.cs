using CadsBridge.Infrastructure.Authentication.Fakes;

namespace CadsBridge.Infrastructure.Authentication.Configuration;

public class StsConfig
{
    public bool Enabled { get; set; } = false;
    public string Audience { get; set; } = string.Empty;
    public StsMockConfig Mock { get; set; } = new();
}