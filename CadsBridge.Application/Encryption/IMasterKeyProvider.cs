namespace CadsBridge.Application.Encryption;

/// <summary>A master key and its identifier. <see cref="Key"/> is exactly 32 random bytes.</summary>
/// <remarks>The caller owns <see cref="Key"/> and should zero it after use.</remarks>
public readonly record struct MasterKey(string Id, byte[] Key);

/// <summary>
/// Supplies master keys. Implementations must return a fresh copy of the key bytes on every call
/// because callers zero the arrays they receive.
/// </summary>
public interface IMasterKeyProvider
{
    /// <summary>The key new files are encrypted under.</summary>
    ValueTask<MasterKey> GetCurrentAsync(CancellationToken cancellationToken = default);

    /// <summary>The key a file was encrypted under, looked up by the id stored in its header.</summary>
    /// <exception cref="KeyNotFoundException">No key with that id exists.</exception>
    ValueTask<byte[]> GetByIdAsync(string keyId, CancellationToken cancellationToken = default);
}