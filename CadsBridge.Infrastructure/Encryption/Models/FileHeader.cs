using CadsBridge.Infrastructure.Encryption.Exceptions;
using System.Buffers.Binary;
using System.Text;

namespace CadsBridge.Infrastructure.Encryption.Models;

/// <summary>Constants that define the on-disk format. Changing any of these is a format change.</summary>
internal static class FileFormat
{
    public static ReadOnlySpan<byte> Magic => "CFE\0"u8;

    public const byte Version1 = 1;

    /// <summary>Algorithm 1: chunked AES-256-GCM; data key wrapped with AES-256-GCM under an HKDF-SHA256 derived key.</summary>
    public const byte AlgorithmAesGcmChunked = 1;

    public const int MasterKeySize = 32;
    public const int DataKeySize = 32;
    public const int NonceSize = 12;
    public const int TagSize = 16;
    public const int NoncePrefixSize = 8;
    public const int HkdfSaltSize = 16;
    public const int MaxKeyIdLength = 32;

    public const uint DefaultChunkSize = 64 * 1024;
    public const uint MinChunkSize = 4 * 1024;
    public const uint MaxChunkSize = 16 * 1024 * 1024;

    /// <summary>HKDF info string; binds derived keys to this purpose and format version.</summary>
    public static ReadOnlySpan<byte> HkdfInfo => "CadsBridge.FileEncryption/v1/file-kek"u8;
}

/// <summary>
/// Fixed-size (127 byte) file header.
/// <code>
///  0   4  magic "CFE\0"
///  4   1  format version
///  5   1  algorithm
///  6   4  chunk size (plaintext bytes per chunk), big-endian
/// 10   8  nonce prefix (random per file)
/// -- bytes 0..18 are the "stable" header: authenticated as AAD on every chunk --
/// 18   1  master key id length
/// 19  32  master key id, zero padded
/// 51  16  HKDF salt (random per wrap)
/// -- bytes 0..67 are authenticated as AAD of the data key wrap --
/// 67  12  wrap nonce (random per wrap)
/// 79  32  wrapped data key
/// 111 16  wrap tag
/// </code>
/// Only bytes 18..127 change when a file is re-wrapped under a different master key,
/// so the body never has to be re-encrypted.
/// </summary>
internal sealed class FileHeader
{
    public const int StableLength = 18;
    public const int WrapAadLength = 67;
    public const int Size = 127;

    private const int OffsetKeyIdLen = 18;
    private const int OffsetKeyId = 19;
    private const int OffsetHkdfSalt = 51;
    private const int OffsetWrapNonce = 67;
    private const int OffsetWrappedKey = 79;
    private const int OffsetWrapTag = 111;

    public byte Version { get; init; } = FileFormat.Version1;
    public byte Algorithm { get; init; } = FileFormat.AlgorithmAesGcmChunked;
    public uint ChunkSize { get; init; }
    public required byte[] NoncePrefix { get; init; }
    public required string KeyId { get; init; }
    public required byte[] HkdfSalt { get; init; }
    public required byte[] WrapNonce { get; init; }
    public required byte[] WrappedKey { get; init; }
    public required byte[] WrapTag { get; init; }

    public static void ValidateKeyId(string keyId)
    {
        ArgumentException.ThrowIfNullOrEmpty(keyId);
        var len = Encoding.UTF8.GetByteCount(keyId);
        if (len > FileFormat.MaxKeyIdLength)
            throw new ArgumentException($"Key id must be at most {FileFormat.MaxKeyIdLength} UTF-8 bytes.", nameof(keyId));
    }

    public byte[] ToBytes()
    {
        ValidateKeyId(KeyId);
        var buf = new byte[Size];
        FileFormat.Magic.CopyTo(buf);
        buf[4] = Version;
        buf[5] = Algorithm;
        BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(6, 4), ChunkSize);
        NoncePrefix.CopyTo(buf.AsSpan(10, FileFormat.NoncePrefixSize));

        var idLen = Encoding.UTF8.GetBytes(KeyId, buf.AsSpan(OffsetKeyId, FileFormat.MaxKeyIdLength));
        buf[OffsetKeyIdLen] = (byte)idLen;

        HkdfSalt.CopyTo(buf.AsSpan(OffsetHkdfSalt, FileFormat.HkdfSaltSize));
        WrapNonce.CopyTo(buf.AsSpan(OffsetWrapNonce, FileFormat.NonceSize));
        WrappedKey.CopyTo(buf.AsSpan(OffsetWrappedKey, FileFormat.DataKeySize));
        WrapTag.CopyTo(buf.AsSpan(OffsetWrapTag, FileFormat.TagSize));
        return buf;
    }

    public static FileHeader Parse(ReadOnlySpan<byte> buf)
    {
        if (buf.Length != Size)
            throw new EncryptedFileFormatException("Header is truncated.");
        if (!buf[..4].SequenceEqual(FileFormat.Magic))
            throw new EncryptedFileFormatException("Not an encrypted file (bad magic).");
        if (buf[4] != FileFormat.Version1)
            throw new EncryptedFileFormatException($"Unsupported format version {buf[4]}.");
        if (buf[5] != FileFormat.AlgorithmAesGcmChunked)
            throw new EncryptedFileFormatException($"Unsupported algorithm {buf[5]}.");

        var chunkSize = BinaryPrimitives.ReadUInt32BigEndian(buf.Slice(6, 4));
        if (chunkSize is < FileFormat.MinChunkSize or > FileFormat.MaxChunkSize)
            throw new EncryptedFileFormatException($"Invalid chunk size {chunkSize}.");

        var idLen = buf[OffsetKeyIdLen];
        if (idLen is 0 or > FileFormat.MaxKeyIdLength)
            throw new EncryptedFileFormatException("Invalid key id length.");

        string keyId;
        try
        {
            keyId = new UTF8Encoding(false, throwOnInvalidBytes: true).GetString(buf.Slice(OffsetKeyId, idLen));
        }
        catch (ArgumentException ex)
        {
            throw new EncryptedFileFormatException("Key id is not valid UTF-8.", ex);
        }

        return new FileHeader
        {
            Version = buf[4],
            Algorithm = buf[5],
            ChunkSize = chunkSize,
            NoncePrefix = buf.Slice(10, FileFormat.NoncePrefixSize).ToArray(),
            KeyId = keyId,
            HkdfSalt = buf.Slice(OffsetHkdfSalt, FileFormat.HkdfSaltSize).ToArray(),
            WrapNonce = buf.Slice(OffsetWrapNonce, FileFormat.NonceSize).ToArray(),
            WrappedKey = buf.Slice(OffsetWrappedKey, FileFormat.DataKeySize).ToArray(),
            WrapTag = buf.Slice(OffsetWrapTag, FileFormat.TagSize).ToArray(),
        };
    }
}