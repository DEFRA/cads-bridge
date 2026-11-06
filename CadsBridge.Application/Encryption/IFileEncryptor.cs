namespace CadsBridge.Application.Encryption;

/// <summary>Streaming authenticated file encryption shared by all services.</summary>
public interface IFileEncryptor
{
    /// <summary>
    /// Encrypts <paramref name="input"/> into <paramref name="output"/>. Neither stream is closed.
    /// </summary>
    /// <param name="totalBytes">Plaintext length, if known, for progress reporting.</param>
    /// <param name="progress">Called with (percent, message).</param>
    Task EncryptAsync(Stream input, Stream output, long? totalBytes = null,
        Action<int, string>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Decrypts <paramref name="input"/> into <paramref name="output"/>. Neither stream is closed.
    /// </summary>
    /// <remarks>
    /// Plaintext is written chunk by chunk as each chunk authenticates, but the file is only known to be
    /// complete and untampered once this method returns without throwing. If it throws
    /// <see cref="EncryptedFileAuthenticationException"/> or <see cref="EncryptedFileFormatException"/>,
    /// discard everything written to <paramref name="output"/>. Prefer
    /// <see cref="FileEncryptorExtensions.DecryptToFileAsync"/>, which does this for you.
    /// </remarks>
    /// <param name="totalBytes">Encrypted length, if known, for progress reporting.</param>
    Task DecryptAsync(Stream input, Stream output, long? totalBytes = null,
        Action<int, string>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Copies an encrypted file, replacing only its header so the data key is wrapped under the provider's
    /// current master key. The body is copied byte for byte and is not decrypted or verified.
    /// </summary>
    Task RewrapAsync(Stream input, Stream output, CancellationToken cancellationToken = default);
}