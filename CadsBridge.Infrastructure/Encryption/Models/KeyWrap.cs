using CadsBridge.Infrastructure.Encryption.Exceptions;
using System.Security.Cryptography;

namespace CadsBridge.Infrastructure.Encryption.Models;

/// <summary>
/// Wraps and unwraps the per-file data key. The wrapping key is
/// HKDF-SHA256(masterKey, salt = per-wrap random, info = fixed label), and the wrap is AES-256-GCM
/// with header bytes 0..67 (format, chunk size, nonce prefix, key id, HKDF salt) as AAD.
/// </summary>
internal static class KeyWrap
{
    public static FileHeader Wrap(
        uint chunkSize,
        byte[] noncePrefix,
        string keyId,
        byte[] masterKey,
        byte[] dataKey,
        Action<Span<byte>> fillRandom)
    {
        var salt = new byte[FileFormat.HkdfSaltSize];
        var nonce = new byte[FileFormat.NonceSize];
        fillRandom(salt);
        fillRandom(nonce);

        var template = new FileHeader
        {
            ChunkSize = chunkSize,
            NoncePrefix = noncePrefix,
            KeyId = keyId,
            HkdfSalt = salt,
            WrapNonce = nonce,
            WrappedKey = new byte[FileFormat.DataKeySize],
            WrapTag = new byte[FileFormat.TagSize],
        };
        var aad = template.ToBytes().AsSpan(0, FileHeader.WrapAadLength);

        var wrapped = new byte[FileFormat.DataKeySize];
        var tag = new byte[FileFormat.TagSize];
        var kek = DeriveKek(masterKey, salt);
        try
        {
            using var gcm = new AesGcm(kek, FileFormat.TagSize);
            gcm.Encrypt(nonce, dataKey, wrapped, tag, aad);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(kek);
        }

        return new FileHeader
        {
            ChunkSize = chunkSize,
            NoncePrefix = noncePrefix,
            KeyId = keyId,
            HkdfSalt = salt,
            WrapNonce = nonce,
            WrappedKey = wrapped,
            WrapTag = tag,
        };
    }

    /// <summary>Returns the data key. The caller must zero it after use.</summary>
    public static byte[] Unwrap(FileHeader header, ReadOnlySpan<byte> rawHeader, byte[] masterKey)
    {
        var dataKey = new byte[FileFormat.DataKeySize];
        var kek = DeriveKek(masterKey, header.HkdfSalt);
        try
        {
            using var gcm = new AesGcm(kek, FileFormat.TagSize);
            gcm.Decrypt(header.WrapNonce, header.WrappedKey, header.WrapTag, dataKey, rawHeader[..FileHeader.WrapAadLength]);
            return dataKey;
        }
        catch (CryptographicException ex)
        {
            CryptographicOperations.ZeroMemory(dataKey);
            throw new EncryptedFileAuthenticationException(
                "Could not unwrap the data key: wrong master key, or the header was modified.", ex);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(kek);
        }
    }

    private static byte[] DeriveKek(byte[] masterKey, ReadOnlySpan<byte> salt)
    {
        if (masterKey.Length != FileFormat.MasterKeySize)
            throw new InvalidOperationException($"Master key must be {FileFormat.MasterKeySize} bytes.");

        var kek = new byte[FileFormat.DataKeySize];
        HKDF.DeriveKey(HashAlgorithmName.SHA256, masterKey, kek, salt, FileFormat.HkdfInfo);
        return kek;
    }
}