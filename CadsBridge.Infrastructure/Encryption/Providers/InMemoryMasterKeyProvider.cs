using CadsBridge.Application.Encryption;
using CadsBridge.Infrastructure.Encryption.Configuration;
using CadsBridge.Infrastructure.Encryption.Models;
using Microsoft.Extensions.Options;

namespace CadsBridge.Infrastructure.Encryption.Providers;

/// <summary>
/// Fixed set of master keys held in memory. Intended for tests and local development.
/// </summary>
public sealed class InMemoryMasterKeyProvider : IMasterKeyProvider
{
    private readonly Dictionary<string, byte[]> _keys;
    private readonly string _currentKeyId;

    public InMemoryMasterKeyProvider(IOptions<EncryptionConfig> options)
        : this(options.Value.CurrentKeyId, options.Value.GetKeys())
    {
    }

    public InMemoryMasterKeyProvider(string currentKeyId, IReadOnlyDictionary<string, byte[]> keys)
    {
        ArgumentException.ThrowIfNullOrEmpty(currentKeyId);
        ArgumentNullException.ThrowIfNull(keys);

        if (!keys.ContainsKey(currentKeyId))
        {
            throw new ArgumentException(
                "Current key id is not in the key set.",
                nameof(currentKeyId));
        }

        _keys = CreateKeySet(keys);
        _currentKeyId = currentKeyId;
    }

    private static Dictionary<string, byte[]> CreateKeySet(
       IReadOnlyDictionary<string, byte[]> keys)
    {
        var keySet = new Dictionary<string, byte[]>(StringComparer.Ordinal);

        foreach (var (id, key) in keys)
        {
            FileHeader.ValidateKeyId(id);

            if (key.Length != FileFormat.MasterKeySize)
            {
                throw new ArgumentException(
                    $"Master key '{id}' must be {FileFormat.MasterKeySize} bytes.",
                    nameof(keys));
            }

            keySet[id] = (byte[])key.Clone();
        }

        return keySet;
    }

    public ValueTask<MasterKey> GetCurrentAsync(CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(new MasterKey(_currentKeyId, (byte[])_keys[_currentKeyId].Clone()));

    public ValueTask<byte[]> GetByIdAsync(string keyId, CancellationToken cancellationToken = default) =>
        _keys.TryGetValue(keyId, out var key)
            ? ValueTask.FromResult((byte[])key.Clone())
            : throw new KeyNotFoundException($"Master key '{keyId}' not found.");
}