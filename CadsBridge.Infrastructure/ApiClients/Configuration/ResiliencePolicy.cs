namespace CadsBridge.Infrastructure.ApiClients.Configuration;

public class ResiliencePolicy
{
    public int Retries { get; set; } = 3; // Total retry attempts
    public int BaseDelaySeconds { get; set; } = 2; // Base delay before first retry
    public bool UseJitter { get; set; } = true; // Adds randomness to avoid thundering herd
    public int TimeoutPeriodSeconds { get; set; } = 30; // Canceling execution if it does not complete within a specified time
}