namespace CadsBridge.Infrastructure.Storage.Configuration;

public record StorageConfiguration
{
    public StorageConfigurationDetails InternalStorage { get; init; } = new();

    public StorageConfigurationsDetailsWithCredentials ExternalStorage { get; init; } = new();
}