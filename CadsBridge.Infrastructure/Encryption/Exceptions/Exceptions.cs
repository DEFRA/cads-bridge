namespace CadsBridge.Infrastructure.Encryption.Exceptions;

/// <summary>
/// The input is not a valid encrypted file: bad magic, unsupported version or algorithm,
/// malformed header, or the stream ended before a complete chunk could be read.
/// </summary>
public sealed class EncryptedFileFormatException : Exception
{
    public EncryptedFileFormatException(string message) : base(message) { }
    public EncryptedFileFormatException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// Authentication failed: the file was modified, truncated, reordered, or decrypted with the wrong key.
/// Any plaintext already written to the output stream must be discarded.
/// </summary>
public sealed class EncryptedFileAuthenticationException : Exception
{
    public EncryptedFileAuthenticationException(string message, Exception inner) : base(message, inner) { }
}