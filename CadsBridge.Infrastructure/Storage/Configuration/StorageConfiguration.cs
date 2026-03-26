namespace CadsBridge.Infrastructure.Storage.Configuration;

public record StorageConfiguration
{
    public StorageConfigurationDetails Internal { get; init; } = new();

    public StorageConfigurationDetails External { get; init; } = new();
}