namespace CadsBridge.Infrastructure.ApiClients.Configuration;

public class ApiClientConfiguration
{
    public string BaseUrl { get; set; } = string.Empty;
    public string BasicApiKey { get; set; } = string.Empty;
    public string? XApiKey { get; set; } = string.Empty;
    public bool HealthcheckEnabled { get; set; } = false;
    public ResiliencePolicy ResiliencePolicy { get; set; } = new();
}