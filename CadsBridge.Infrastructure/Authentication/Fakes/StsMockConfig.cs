namespace CadsBridge.Infrastructure.Authentication.Fakes;

public class StsMockConfig
{
    public bool Enabled { get; set; } = false;
    public string TokenEndpoint { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public string ClientSecret { get; set; } = string.Empty;
    public string Scope { get; set; } = string.Empty;
}