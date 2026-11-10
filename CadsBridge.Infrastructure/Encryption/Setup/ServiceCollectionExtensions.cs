using CadsBridge.Application.Encryption;
using CadsBridge.Infrastructure.Encryption.Configuration;
using CadsBridge.Infrastructure.Encryption.Providers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Security.Cryptography;
using System.Text;

namespace CadsBridge.Infrastructure.Encryption.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEncryption(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<EncryptionConfig>()
            .Bind(configuration.GetSection(EncryptionConfig.SectionName))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        // Fails the app at startup (via ValidateOnStart) if the keys are missing or malformed.
        services.AddSingleton<IValidateOptions<EncryptionConfig>, EncryptionConfigValidator>();
        services.AddSingleton<IMasterKeyProvider>(sp =>
        {
            var config = sp.GetRequiredService<IOptions<EncryptionConfig>>().Value;

            return config.StorageEncryptionEnabled
                ? new InMemoryMasterKeyProvider(config.CurrentKeyId, config.GetKeys())
                : new DisabledMasterKeyProvider();
        });
        services.AddSingleton<IFileEncryptor>(sp => new FileEncryptor(sp.GetRequiredService<IMasterKeyProvider>()));

        return services;
    }

    private sealed class DisabledMasterKeyProvider : IMasterKeyProvider
    {
        private static InvalidOperationException Disabled() =>
            new("Storage encryption is disabled, so no master keys are loaded.");

        public ValueTask<MasterKey> GetCurrentAsync(CancellationToken cancellationToken = default) => throw Disabled();
        public ValueTask<byte[]> GetByIdAsync(string keyId, CancellationToken cancellationToken = default) => throw Disabled();
    }
}

/// <summary>
/// Validates the key material when storage encryption is enabled. Never puts key values in error messages.
/// </summary>
public sealed class EncryptionConfigValidator : IValidateOptions<EncryptionConfig>
{
    private const int KeySize = 32;
    private const int MaxKeyIdBytes = 32;

    public ValidateOptionsResult Validate(string? name, EncryptionConfig options)
    {
        if (!options.StorageEncryptionEnabled)
            return ValidateOptionsResult.Success;

        var errors = new List<string>();

        if (options.Keys is not { Count: > 0 })
        {
            errors.Add("Encryption:Keys must contain at least one key when storage encryption is enabled.");
            return ValidateOptionsResult.Fail(errors);
        }

        if (!options.Keys.ContainsKey(options.CurrentKeyId))
            errors.Add($"Encryption:CurrentKeyId '{options.CurrentKeyId}' is not present in Encryption:Keys.");

        var buffer = new byte[KeySize * 2];
        foreach (var (id, value) in options.Keys)
        {
            var idBytes = Encoding.UTF8.GetByteCount(id);
            if (idBytes is 0 or > MaxKeyIdBytes)
                errors.Add($"Encryption:Keys:'{id}': key ids must be 1 to {MaxKeyIdBytes} UTF-8 bytes.");

            if (!Convert.TryFromBase64String(value ?? string.Empty, buffer, out var written) || written != KeySize)
                errors.Add($"Encryption:Keys:{id} must be the base64 encoding of exactly {KeySize} random bytes (for example `openssl rand -base64 32`).");

            CryptographicOperations.ZeroMemory(buffer);
        }

        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}