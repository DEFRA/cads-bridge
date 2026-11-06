using CadsBridge.Application.Encryption;
using CadsBridge.Infrastructure.Encryption.Exceptions;
using CadsBridge.Infrastructure.Encryption.Models;
using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;

namespace CadsBridge.Infrastructure.Encryption;

/// <summary>
/// Chunked AES-256-GCM file encryption. See README.md for the format.
/// Thread-safe: instances hold no per-call state.
/// </summary>
public sealed class FileEncryptor : IFileEncryptor
{
    private readonly IMasterKeyProvider _keys;
    private readonly uint _chunkSize;
    private readonly Action<Span<byte>> _fillRandom;

    public FileEncryptor(IMasterKeyProvider keys, uint chunkSize = FileFormat.DefaultChunkSize)
        : this(keys, chunkSize, RandomNumberGenerator.Fill)
    {
    }

    /// <summary>Test seam: lets tests supply deterministic randomness to pin the format.</summary>
    internal FileEncryptor(IMasterKeyProvider keys, uint chunkSize, Action<Span<byte>> fillRandom)
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (chunkSize is < FileFormat.MinChunkSize or > FileFormat.MaxChunkSize)
            throw new ArgumentOutOfRangeException(nameof(chunkSize),
                $"Chunk size must be between {FileFormat.MinChunkSize} and {FileFormat.MaxChunkSize}.");
        _keys = keys;
        _chunkSize = chunkSize;
        _fillRandom = fillRandom;
    }

    public async Task EncryptAsync(Stream input, Stream output, long? totalBytes = null,
        Action<int, string>? progress = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);

        var master = await _keys.GetCurrentAsync(cancellationToken).ConfigureAwait(false);
        var dataKey = new byte[FileFormat.DataKeySize];
        try
        {
            _fillRandom(dataKey);
            var noncePrefix = new byte[FileFormat.NoncePrefixSize];
            _fillRandom(noncePrefix);

            var header = KeyWrap.Wrap(_chunkSize, noncePrefix, master.Id, master.Key, dataKey, _fillRandom);
            var headerBytes = header.ToBytes();

            var reporter = new ProgressReporter(totalBytes, progress, "Encrypting");
            reporter.Started();
            await output.WriteAsync(headerBytes, cancellationToken).ConfigureAwait(false);
            await EncryptBodyAsync(input, output, headerBytes, noncePrefix, (int)_chunkSize, dataKey, reporter, cancellationToken)
                .ConfigureAwait(false);
            reporter.Completed();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(dataKey);
            CryptographicOperations.ZeroMemory(master.Key);
        }
    }

    public async Task DecryptAsync(Stream input, Stream output, long? totalBytes = null,
        Action<int, string>? progress = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);

        var (header, rawHeader) = await ReadHeaderAsync(input, cancellationToken).ConfigureAwait(false);
        var dataKey = await UnwrapAsync(header, rawHeader, cancellationToken).ConfigureAwait(false);
        try
        {
            var reporter = new ProgressReporter(totalBytes, progress, "Decrypting");
            reporter.Started();
            reporter.Advance(FileHeader.Size);
            await DecryptBodyAsync(input, output, rawHeader, header.NoncePrefix, (int)header.ChunkSize, dataKey, reporter, cancellationToken)
                .ConfigureAwait(false);
            reporter.Completed();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(dataKey);
        }
    }

    public async Task RewrapAsync(Stream input, Stream output, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);

        var (header, rawHeader) = await ReadHeaderAsync(input, cancellationToken).ConfigureAwait(false);
        var dataKey = await UnwrapAsync(header, rawHeader, cancellationToken).ConfigureAwait(false);
        var master = await _keys.GetCurrentAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var newHeader = KeyWrap.Wrap(header.ChunkSize, header.NoncePrefix, master.Id, master.Key, dataKey, _fillRandom);
            await output.WriteAsync(newHeader.ToBytes(), cancellationToken).ConfigureAwait(false);
            await input.CopyToAsync(output, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(dataKey);
            CryptographicOperations.ZeroMemory(master.Key);
        }
    }

    // ---- header / key helpers ---------------------------------------------------------------------

    private static async Task<(FileHeader Header, byte[] Raw)> ReadHeaderAsync(Stream input, CancellationToken ct)
    {
        var raw = new byte[FileHeader.Size];
        var read = await input.ReadAtLeastAsync(raw, raw.Length, throwOnEndOfStream: false, ct).ConfigureAwait(false);
        if (read < raw.Length)
            throw new EncryptedFileFormatException("File is too short to contain a header.");
        return (FileHeader.Parse(raw), raw);
    }

    private async Task<byte[]> UnwrapAsync(FileHeader header, byte[] rawHeader, CancellationToken ct)
    {
        var masterKey = await _keys.GetByIdAsync(header.KeyId, ct).ConfigureAwait(false);
        try
        {
            return KeyWrap.Unwrap(header, rawHeader, masterKey);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(masterKey);
        }
    }

    // ---- body ---------------------------------------------------------------------------------------

    private static async Task EncryptBodyAsync(Stream input, Stream output, byte[] rawHeader, byte[] noncePrefix,
        int chunkSize, byte[] dataKey, ProgressReporter reporter, CancellationToken ct)
    {
        var pool = ArrayPool<byte>.Shared;
        var cur = pool.Rent(chunkSize);
        var next = pool.Rent(chunkSize);
        var record = pool.Rent(chunkSize + FileFormat.TagSize);
        try
        {
            using var gcm = new AesGcm(dataKey, FileFormat.TagSize);

            var curLen = await input.ReadAtLeastAsync(cur.AsMemory(0, chunkSize), chunkSize, false, ct).ConfigureAwait(false);
            reporter.Advance(curLen);
            uint index = 0;

            while (true)
            {
                // A chunk is final if it is short, or full and followed by EOF. An empty input produces one empty final chunk.
                var nextLen = 0;
                if (curLen == chunkSize)
                {
                    nextLen = await input.ReadAtLeastAsync(next.AsMemory(0, chunkSize), chunkSize, false, ct).ConfigureAwait(false);
                    reporter.Advance(nextLen);
                }
                var isFinal = nextLen == 0;

                SealChunk(gcm, rawHeader, noncePrefix, index, isFinal,
                    cur.AsSpan(0, curLen),
                    record.AsSpan(0, curLen),
                    record.AsSpan(curLen, FileFormat.TagSize));
                await output.WriteAsync(record.AsMemory(0, curLen + FileFormat.TagSize), ct).ConfigureAwait(false);

                if (isFinal) break;
                if (index == uint.MaxValue)
                    throw new InvalidOperationException("File is too large for this format (chunk counter exhausted).");

                (cur, next) = (next, cur);
                curLen = nextLen;
                index++;
            }
        }
        finally
        {
            pool.Return(cur, clearArray: true);
            pool.Return(next, clearArray: true);
            pool.Return(record, clearArray: true);
        }
    }

    private static async Task DecryptBodyAsync(Stream input, Stream output, byte[] rawHeader, byte[] noncePrefix,
        int chunkSize, byte[] dataKey, ProgressReporter reporter, CancellationToken ct)
    {
        var recordSize = chunkSize + FileFormat.TagSize;
        var pool = ArrayPool<byte>.Shared;
        var cur = pool.Rent(recordSize);
        var next = pool.Rent(recordSize);
        var plain = pool.Rent(chunkSize);
        try
        {
            using var gcm = new AesGcm(dataKey, FileFormat.TagSize);

            var curLen = await input.ReadAtLeastAsync(cur.AsMemory(0, recordSize), recordSize, false, ct).ConfigureAwait(false);
            reporter.Advance(curLen);
            uint index = 0;

            while (true)
            {
                if (curLen < FileFormat.TagSize)
                    throw new EncryptedFileFormatException("File is truncated (incomplete final chunk).");

                var nextLen = 0;
                if (curLen == recordSize)
                {
                    nextLen = await input.ReadAtLeastAsync(next.AsMemory(0, recordSize), recordSize, false, ct).ConfigureAwait(false);
                    reporter.Advance(nextLen);
                }
                var isFinal = nextLen == 0;

                var plainLen = curLen - FileFormat.TagSize;
                try
                {
                    OpenChunk(gcm, rawHeader, noncePrefix, index, isFinal,
                        cur.AsSpan(0, plainLen),
                        cur.AsSpan(plainLen, FileFormat.TagSize),
                        plain.AsSpan(0, plainLen));
                }
                catch (CryptographicException ex)
                {
                    throw new EncryptedFileAuthenticationException(
                        $"Chunk {index} failed authentication: the file is corrupt, truncated, reordered or was modified.", ex);
                }

                await output.WriteAsync(plain.AsMemory(0, plainLen), ct).ConfigureAwait(false);

                if (isFinal) break;
                if (index == uint.MaxValue)
                    throw new EncryptedFileFormatException("Chunk counter exhausted.");

                (cur, next) = (next, cur);
                curLen = nextLen;
                index++;
            }
        }
        finally
        {
            pool.Return(cur, clearArray: true);
            pool.Return(next, clearArray: true);
            pool.Return(plain, clearArray: true);
        }
    }

    // Spans stay in these synchronous helpers because they cannot live across an await.

    private static void SealChunk(AesGcm gcm, ReadOnlySpan<byte> rawHeader, ReadOnlySpan<byte> noncePrefix,
        uint index, bool isFinal, ReadOnlySpan<byte> plaintext, Span<byte> ciphertext, Span<byte> tag)
    {
        Span<byte> nonce = stackalloc byte[FileFormat.NonceSize];
        Span<byte> aad = stackalloc byte[FileHeader.StableLength + 5];
        BuildNonceAndAad(rawHeader, noncePrefix, index, isFinal, nonce, aad);
        gcm.Encrypt(nonce, plaintext, ciphertext, tag, aad);
    }

    private static void OpenChunk(AesGcm gcm, ReadOnlySpan<byte> rawHeader, ReadOnlySpan<byte> noncePrefix,
        uint index, bool isFinal, ReadOnlySpan<byte> ciphertext, ReadOnlySpan<byte> tag, Span<byte> plaintext)
    {
        Span<byte> nonce = stackalloc byte[FileFormat.NonceSize];
        Span<byte> aad = stackalloc byte[FileHeader.StableLength + 5];
        BuildNonceAndAad(rawHeader, noncePrefix, index, isFinal, nonce, aad);
        gcm.Decrypt(nonce, ciphertext, tag, plaintext, aad);
    }

    private static void BuildNonceAndAad(ReadOnlySpan<byte> rawHeader, ReadOnlySpan<byte> noncePrefix,
        uint index, bool isFinal, Span<byte> nonce, Span<byte> aad)
    {
        // nonce = noncePrefix(8) || index(4, big-endian)
        noncePrefix.CopyTo(nonce);
        BinaryPrimitives.WriteUInt32BigEndian(nonce[FileFormat.NoncePrefixSize..], index);

        // aad = stable header(18) || index(4, big-endian) || final flag(1)
        rawHeader[..FileHeader.StableLength].CopyTo(aad);
        BinaryPrimitives.WriteUInt32BigEndian(aad[FileHeader.StableLength..], index);
        aad[FileHeader.StableLength + 4] = isFinal ? (byte)1 : (byte)0;
    }

    // ---- progress -----------------------------------------------------------------------------------

    private sealed class ProgressReporter(long? total, Action<int, string>? callback, string operation)
    {
        private long _processed;
        private int _lastPercent = -1;

        public void Started() => callback?.Invoke(0, $"{operation} started");

        public void Advance(long bytes)
        {
            _processed += bytes;
            if (callback is null) return;

            if (total is > 0)
            {
                var percent = (int)Math.Min(99, _processed * 100 / total.Value);
                if (percent == _lastPercent) return;
                _lastPercent = percent;
                callback(percent, $"{operation} {percent}% - {FormatBytes(_processed)} of {FormatBytes(total.Value)}");
            }
            else
            {
                callback(0, $"{operation} - {FormatBytes(_processed)} processed");
            }
        }

        public void Completed() =>
            callback?.Invoke(100, $"{operation} completed - {FormatBytes(_processed)} processed");

        private static string FormatBytes(long bytes) => bytes switch
        {
            >= 1L << 30 => $"{bytes / (double)(1L << 30):F2} GB",
            >= 1L << 20 => $"{bytes / (double)(1L << 20):F2} MB",
            >= 1L << 10 => $"{bytes / (double)(1L << 10):F2} KB",
            _ => $"{bytes} bytes",
        };
    }
}