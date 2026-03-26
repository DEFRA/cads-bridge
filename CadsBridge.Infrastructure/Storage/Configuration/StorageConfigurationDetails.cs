namespace CadsBridge.Infrastructure.Storage.Configuration;

public record StorageConfigurationDetails
{
    public bool HealthcheckEnabled { get; init; }
    public string BucketName { get; init; } = string.Empty;
}