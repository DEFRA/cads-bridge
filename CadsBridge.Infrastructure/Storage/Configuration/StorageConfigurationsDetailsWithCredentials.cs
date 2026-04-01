namespace CadsBridge.Infrastructure.Storage.Configuration;

public record StorageConfigurationsDetailsWithCredentials
    : StorageConfigurationDetails
{
    public string AccessKeySecretName { get; init; } = string.Empty;
    public string SecretKeySecretName { get; init; } = string.Empty;
}