namespace CadsBridge.Infrastructure.ApiClients.Configuration;

public class StsConfig
{
    public bool Enabled { get; set; } = false;
    public string Audience { get; set; } = string.Empty;
}