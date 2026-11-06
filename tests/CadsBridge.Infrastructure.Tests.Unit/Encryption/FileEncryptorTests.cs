using CadsBridge.Application.Encryption;
using CadsBridge.Application.Extensions;
using CadsBridge.Infrastructure.Encryption;
using CadsBridge.Infrastructure.Encryption.Exceptions;
using CadsBridge.Infrastructure.Encryption.Models;
using CadsBridge.Infrastructure.Encryption.Providers;
using System.Security.Cryptography;

namespace CadsBridge.Infrastructure.Tests.Unit.Encryption;

public class FileEncryptorTests
{
    private const uint SmallChunk = FileFormat.MinChunkSize; // 4096, keeps multi-chunk tests small

    private static byte[] Key(byte seed) => [.. Enumerable.Range(0, 32).Select(i => (byte)(seed + i))];

    private static InMemoryMasterKeyProvider Keys(string current = "k1", params string[] ids)
    {
        var all = (ids.Length == 0 ? ["k1"] : ids).Distinct().ToDictionary(id => id, id => Key((byte)id.GetHashCode()));
        return new InMemoryMasterKeyProvider(current, all);
    }

    private static FileEncryptor Enc(IMasterKeyProvider? keys = null, uint chunk = SmallChunk) =>
        new(keys ?? Keys(), chunk);

    private static byte[] Data(int length, int seed = 1)
    {
        var data = new byte[length];
        new Random(seed).NextBytes(data);
        return data;
    }

    private static async Task<byte[]> EncryptAsync(FileEncryptor enc, byte[] plaintext)
    {
        using var input = new MemoryStream(plaintext);
        using var output = new MemoryStream();
        await enc.EncryptAsync(input, output);
        return output.ToArray();
    }

    private static async Task<byte[]> DecryptAsync(FileEncryptor enc, byte[] ciphertext)
    {
        using var input = new MemoryStream(ciphertext);
        using var output = new MemoryStream();
        await enc.DecryptAsync(input, output);
        return output.ToArray();
    }

    private static bool IsExpectedFailure(Exception ex) =>
        ex is EncryptedFileFormatException or EncryptedFileAuthenticationException or KeyNotFoundException;

    // ---- round trips ------------------------------------------------------------------------------

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(4095)]
    [InlineData(4096)]
    [InlineData(4097)]
    [InlineData(8192)]
    [InlineData(3 * 4096 + 17)]
    [InlineData(10 * 4096)]
    public async Task RoundTrips_AroundChunkBoundaries(int length)
    {
        var enc = Enc();
        var plaintext = Data(length);

        var ciphertext = await EncryptAsync(enc, plaintext);
        var decrypted = await DecryptAsync(enc, ciphertext);

        Assert.Equal(plaintext, decrypted);
    }

    [Fact]
    public async Task RoundTrips_WithDefaultChunkSize()
    {
        var enc = new FileEncryptor(Keys());
        var plaintext = Data(300_000);
        Assert.Equal(plaintext, await DecryptAsync(enc, await EncryptAsync(enc, plaintext)));
    }

    [Fact]
    public async Task CiphertextSize_IsHeaderPlusPlaintextPlusOneTagPerChunk()
    {
        var enc = Enc();
        Assert.Equal(FileHeader.Size + 16, (await EncryptAsync(enc, [])).Length);                 // empty -> one final chunk
        Assert.Equal(FileHeader.Size + 4096 + 16, (await EncryptAsync(enc, Data(4096))).Length);   // exactly one full chunk
        Assert.Equal(FileHeader.Size + 4097 + 32, (await EncryptAsync(enc, Data(4097))).Length);   // two chunks
    }

    [Fact]
    public async Task SamePlaintext_ProducesDifferentCiphertexts()
    {
        var enc = Enc();
        var plaintext = Data(5000);
        Assert.NotEqual(await EncryptAsync(enc, plaintext), await EncryptAsync(enc, plaintext));
    }

    [Fact]
    public async Task IdenticalPlaintextChunks_ProduceDifferentCiphertextChunks()
    {
        // The property ECB lacks: repeated plaintext blocks must not repeat in the ciphertext.
        var enc = Enc();
        var plaintext = new byte[4096 * 3];
        var ciphertext = await EncryptAsync(enc, plaintext);
        var c0 = ciphertext.AsSpan(FileHeader.Size, 4096).ToArray();
        var c1 = ciphertext.AsSpan(FileHeader.Size + 4096 + 16, 4096).ToArray();
        Assert.NotEqual(c0, c1);
    }

    [Fact]
    public async Task LargeStream_RoundTripsWithoutBufferingTheFile()
    {
        const long size = 64L * 1024 * 1024;
        var enc = new FileEncryptor(Keys());

        using var encrypted = new MemoryStream();
        await enc.EncryptAsync(new PatternStream(size), encrypted, cancellationToken: TestContext.Current.CancellationToken);
        encrypted.Position = 0;

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        await enc.DecryptAsync(encrypted, new HashingSink(hash), cancellationToken: TestContext.Current.CancellationToken);

        using var expected = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buf = new byte[1 << 20];
        var src = new PatternStream(size);
        int n;
        while ((n = src.Read(buf, 0, buf.Length)) > 0) expected.AppendData(buf, 0, n);

        Assert.Equal(expected.GetHashAndReset(), hash.GetHashAndReset());
    }

    [Fact]
    public async Task WorksOnNonSeekableStreams()
    {
        var enc = Enc();
        var plaintext = Data(20_000);

        using var encrypted = new MemoryStream();
        await enc.EncryptAsync(new NonSeekableStream(new MemoryStream(plaintext)), new NonSeekableStream(encrypted), cancellationToken: TestContext.Current.CancellationToken);

        using var decrypted = new MemoryStream();
        await enc.DecryptAsync(new NonSeekableStream(new MemoryStream(encrypted.ToArray())), new NonSeekableStream(decrypted), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(plaintext, decrypted.ToArray());
    }

    // ---- tamper detection ---------------------------------------------------------------------------

    [Fact]
    public async Task FlippingAnySingleByteIsDetected_SingleChunkFile()
    {
        var enc = Enc();
        var ciphertext = await EncryptAsync(enc, Data(10));
        await AssertEveryByteFlipFails(enc, ciphertext);
    }

    [Fact]
    public async Task FlippingAnySingleByteIsDetected_MultiChunkFile()
    {
        var enc = Enc();
        var ciphertext = await EncryptAsync(enc, Data(2 * 4096 + 100));
        await AssertEveryByteFlipFails(enc, ciphertext);
    }

    private static async Task AssertEveryByteFlipFails(FileEncryptor enc, byte[] ciphertext)
    {
        for (var i = 0; i < ciphertext.Length; i++)
        {
            var copy = (byte[])ciphertext.Clone();
            copy[i] ^= 0x01;
            var ex = await Record.ExceptionAsync(() => DecryptAsync(enc, copy));
            Assert.True(ex is not null && IsExpectedFailure(ex), $"Flipping byte {i} was not detected (got {ex?.GetType().Name ?? "no exception"}).");
        }
    }

    [Fact]
    public async Task Truncation_AtAnyLengthIsDetected()
    {
        var enc = Enc();
        var ciphertext = await EncryptAsync(enc, Data(2 * 4096 + 100));

        for (var len = 0; len < ciphertext.Length; len++)
        {
            var ex = await Record.ExceptionAsync(() => DecryptAsync(enc, ciphertext[..len]));
            Assert.True(ex is not null && IsExpectedFailure(ex), $"Truncation to {len} bytes was not detected.");
        }
    }

    [Fact]
    public async Task Truncation_AtChunkBoundaryIsDetected()
    {
        // The case a naive chunked scheme misses: the file is cut exactly between two chunks.
        var enc = Enc();
        var ciphertext = await EncryptAsync(enc, Data(3 * 4096));
        var boundary = FileHeader.Size + 2 * (4096 + 16);

        await Assert.ThrowsAsync<EncryptedFileAuthenticationException>(() => DecryptAsync(enc, ciphertext[..boundary]));
    }

    [Fact]
    public async Task AppendedData_IsDetected()
    {
        var enc = Enc();
        foreach (var length in new[] { 0, 50, 4096, 5000 })
        {
            var ciphertext = await EncryptAsync(enc, Data(length));
            foreach (var extra in new[] { 1, 16, 17, 4096 + 16, 4096 + 17 })
            {
                var longer = ciphertext.Concat(new byte[extra]).ToArray();
                var ex = await Record.ExceptionAsync(() => DecryptAsync(enc, longer));
                Assert.True(ex is not null && IsExpectedFailure(ex), $"Appending {extra} bytes to {length}-byte file not detected.");
            }
        }
    }

    [Fact]
    public async Task ReorderedChunks_AreDetected()
    {
        var enc = Enc();
        var ciphertext = await EncryptAsync(enc, Data(3 * 4096 + 5));
        const int record = 4096 + 16;

        var swapped = (byte[])ciphertext.Clone();
        ciphertext.AsSpan(FileHeader.Size + record, record).CopyTo(swapped.AsSpan(FileHeader.Size));
        ciphertext.AsSpan(FileHeader.Size, record).CopyTo(swapped.AsSpan(FileHeader.Size + record));

        await Assert.ThrowsAsync<EncryptedFileAuthenticationException>(() => DecryptAsync(enc, swapped));
    }

    [Fact]
    public async Task ChunkFromAnotherFile_IsRejected()
    {
        var enc = Enc();
        var a = await EncryptAsync(enc, Data(2 * 4096 + 5, seed: 1));
        var b = await EncryptAsync(enc, Data(2 * 4096 + 5, seed: 2));
        const int record = 4096 + 16;

        var spliced = (byte[])a.Clone();
        b.AsSpan(FileHeader.Size, record).CopyTo(spliced.AsSpan(FileHeader.Size));

        await Assert.ThrowsAsync<EncryptedFileAuthenticationException>(() => DecryptAsync(enc, spliced));
    }

    [Fact]
    public async Task RandomGarbage_IsRejectedAsFormatError()
    {
        var enc = Enc();
        await Assert.ThrowsAsync<EncryptedFileFormatException>(() => DecryptAsync(enc, Data(500)));
        await Assert.ThrowsAsync<EncryptedFileFormatException>(() => DecryptAsync(enc, []));
    }

    [Fact]
    public async Task HostileChunkSizeInHeader_IsRejectedBeforeAllocating()
    {
        var enc = Enc();
        var ciphertext = await EncryptAsync(enc, Data(10));
        ciphertext[6] = 0xFF; ciphertext[7] = 0xFF; ciphertext[8] = 0xFF; ciphertext[9] = 0xFF;

        await Assert.ThrowsAsync<EncryptedFileFormatException>(() => DecryptAsync(enc, ciphertext));
    }

    // ---- keys ---------------------------------------------------------------------------------------

    [Fact]
    public async Task WrongMasterKey_FailsAuthentication()
    {
        var ciphertext = await EncryptAsync(Enc(Keys("k1", "k1")), Data(100));

        var other = new InMemoryMasterKeyProvider("k1", new Dictionary<string, byte[]> { ["k1"] = Key(200) });
        await Assert.ThrowsAsync<EncryptedFileAuthenticationException>(() => DecryptAsync(Enc(other), ciphertext));
    }

    [Fact]
    public async Task UnknownKeyId_ThrowsKeyNotFound()
    {
        var ciphertext = await EncryptAsync(Enc(Keys("k1", "k1")), Data(100));

        var other = new InMemoryMasterKeyProvider("k9", new Dictionary<string, byte[]> { ["k9"] = Key(9) });
        await Assert.ThrowsAsync<KeyNotFoundException>(() => DecryptAsync(Enc(other), ciphertext));
    }

    [Fact]
    public async Task TwoServicesWithTheSameKeys_CanReadEachOthersFiles()
    {
        var keys = new Dictionary<string, byte[]> { ["k1"] = Key(1) };
        var serviceA = Enc(new InMemoryMasterKeyProvider("k1", keys));
        var serviceB = Enc(new InMemoryMasterKeyProvider("k1", keys), chunk: 8192); // chunk size comes from the header
        var plaintext = Data(20_000);

        Assert.Equal(plaintext, await DecryptAsync(serviceB, await EncryptAsync(serviceA, plaintext)));
        Assert.Equal(plaintext, await DecryptAsync(serviceA, await EncryptAsync(serviceB, plaintext)));
    }

    [Fact]
    public async Task Rewrap_MovesFileToNewKey_WithoutTouchingTheBody()
    {
        var oldKeys = new Dictionary<string, byte[]> { ["old"] = Key(1) };
        var newKeys = new Dictionary<string, byte[]> { ["old"] = Key(1), ["new"] = Key(2) };
        var plaintext = Data(3 * 4096 + 9);

        var original = await EncryptAsync(Enc(new InMemoryMasterKeyProvider("old", oldKeys)), plaintext);

        var rotating = Enc(new InMemoryMasterKeyProvider("new", newKeys));
        using var rewrapped = new MemoryStream();
        await rotating.RewrapAsync(new MemoryStream(original), rewrapped, TestContext.Current.CancellationToken);
        var rewrappedBytes = rewrapped.ToArray();

        Assert.Equal(original.AsSpan(FileHeader.Size).ToArray(), rewrappedBytes.AsSpan(FileHeader.Size).ToArray());
        Assert.NotEqual(original.AsSpan(0, FileHeader.Size).ToArray(), rewrappedBytes.AsSpan(0, FileHeader.Size).ToArray());
        Assert.Equal("new", FileHeader.Parse(rewrappedBytes.AsSpan(0, FileHeader.Size)).KeyId);

        // The old key can now be retired.
        var newOnly = Enc(new InMemoryMasterKeyProvider("new", new Dictionary<string, byte[]> { ["new"] = Key(2) }));
        Assert.Equal(plaintext, await DecryptAsync(newOnly, rewrappedBytes));
        await Assert.ThrowsAsync<KeyNotFoundException>(() => DecryptAsync(newOnly, original));
    }

    [Fact]
    public async Task Rewrap_OfTamperedHeader_Fails()
    {
        var enc = Enc();
        var ciphertext = await EncryptAsync(enc, Data(100));
        ciphertext[FileHeader.Size - 1] ^= 1; // wrap tag

        await Assert.ThrowsAsync<EncryptedFileAuthenticationException>(
            () => enc.RewrapAsync(new MemoryStream(ciphertext), new MemoryStream(), TestContext.Current.CancellationToken));
    }

    // ---- format pinning -----------------------------------------------------------------------------

    /// <summary>Deterministic "random": 0,1,2,... so the output is byte-for-byte reproducible.</summary>
    private static Action<Span<byte>> Counter()
    {
        byte next = 0;
        return span => { for (var i = 0; i < span.Length; i++) span[i] = next++; };
    }

    internal static byte[] GoldenCiphertext()
    {
        var keys = new InMemoryMasterKeyProvider("golden-key",
            new Dictionary<string, byte[]> { ["golden-key"] = Enumerable.Range(0, 32).Select(i => (byte)(0xA0 + i)).ToArray() });
        var enc = new FileEncryptor(keys, FileFormat.MinChunkSize, Counter());
        var plaintext = Enumerable.Range(0, 5000).Select(i => (byte)(i * 7 + 3)).ToArray(); // 2 chunks: 4096 + 904

        using var output = new MemoryStream();
        enc.EncryptAsync(new MemoryStream(plaintext), output).GetAwaiter().GetResult();
        return output.ToArray();
    }

    [Fact]
    public void GoldenFile_IsByteForByteStable()
    {
        // If this fails you changed the on-disk format. Every existing file becomes unreadable unless you bump the version.
        var actual = SHA256.HashData(GoldenCiphertext());
        Assert.Equal(GoldenVector.Sha256Hex, Convert.ToHexString(actual));
    }

    [Fact]
    public async Task GoldenFile_StillDecrypts()
    {
        var keys = new InMemoryMasterKeyProvider("golden-key",
            new Dictionary<string, byte[]> { ["golden-key"] = Enumerable.Range(0, 32).Select(i => (byte)(0xA0 + i)).ToArray() });
        var plaintext = Enumerable.Range(0, 5000).Select(i => (byte)(i * 7 + 3)).ToArray();

        Assert.Equal(plaintext, await DecryptAsync(new FileEncryptor(keys), GoldenCiphertext()));
    }

    // ---- API behaviour --------------------------------------------------------------------------------

    [Fact]
    public async Task Progress_IsMonotonicAndEndsAt100()
    {
        var enc = Enc();
        var plaintext = Data(10 * 4096);
        var reports = new List<int>();

        await enc.EncryptAsync(new MemoryStream(plaintext), new MemoryStream(), plaintext.Length, (p, _) => reports.Add(p), TestContext.Current.CancellationToken);

        Assert.Equal(0, reports[0]);
        Assert.Equal(100, reports[^1]);
        Assert.Equal(reports.OrderBy(x => x), reports);
    }

    [Fact]
    public async Task Cancellation_IsHonoured()
    {
        var enc = Enc();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => enc.EncryptAsync(new MemoryStream(Data(100_000)), new MemoryStream(), cancellationToken: cts.Token));
    }

    [Fact]
    public async Task StreamsAreLeftOpen()
    {
        var enc = Enc();
        var input = new MemoryStream(Data(100));
        var output = new MemoryStream();
        await enc.EncryptAsync(input, output, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(input.CanRead);
        Assert.True(output.CanWrite);
    }

    [Theory]
    [InlineData(0u)]
    [InlineData(4095u)]
    [InlineData(16u * 1024 * 1024 + 1)]
    public void InvalidChunkSize_IsRejected(uint chunk) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new FileEncryptor(Keys(), chunk));

    [Fact]
    public void KeyIdLongerThan32Bytes_IsRejected() =>
        Assert.Throws<ArgumentException>(() => new InMemoryMasterKeyProvider(new string('k', 33),
            new Dictionary<string, byte[]> { [new string('k', 33)] = Key(1) }));

    [Fact]
    public void MasterKeyOfWrongLength_IsRejected() =>
        Assert.Throws<ArgumentException>(() => new InMemoryMasterKeyProvider("k",
            new Dictionary<string, byte[]> { ["k"] = new byte[16] }));

    // ---- DecryptToFileAsync -----------------------------------------------------------------------------

    [Fact]
    public async Task DecryptToFile_WritesDestinationOnSuccess()
    {
        var enc = Enc();
        var plaintext = Data(9000);
        var ciphertext = await EncryptAsync(enc, plaintext);
        using var dir = new TempDir();
        var dest = Path.Combine(dir.Path, "out.bin");

        await enc.DecryptToFileAsync(new MemoryStream(ciphertext), dest, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(plaintext, await File.ReadAllBytesAsync(dest, TestContext.Current.CancellationToken));
        Assert.Single(Directory.GetFiles(dir.Path));
    }

    [Fact]
    public async Task DecryptToFile_LeavesNothingBehindOnTamper()
    {
        var enc = Enc();
        var ciphertext = await EncryptAsync(enc, Data(3 * 4096));
        ciphertext[^1] ^= 1; // corrupt the last tag, after earlier chunks were already written to the temp file
        using var dir = new TempDir();
        var dest = Path.Combine(dir.Path, "out.bin");

        await Assert.ThrowsAsync<EncryptedFileAuthenticationException>(
            () => enc.DecryptToFileAsync(new MemoryStream(ciphertext), dest, cancellationToken: TestContext.Current.CancellationToken));

        Assert.Empty(Directory.GetFiles(dir.Path));
    }

    [Fact]
    public async Task DecryptToFile_DoesNotClobberExistingFileOnFailure()
    {
        var enc = Enc();
        var ciphertext = await EncryptAsync(enc, Data(3 * 4096));
        ciphertext[^1] ^= 1;
        using var dir = new TempDir();
        var dest = Path.Combine(dir.Path, "out.bin");
        await File.WriteAllTextAsync(dest, "previous", TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<EncryptedFileAuthenticationException>(
            () => enc.DecryptToFileAsync(new MemoryStream(ciphertext), dest, cancellationToken: TestContext.Current.CancellationToken));

        Assert.Equal("previous", await File.ReadAllTextAsync(dest, TestContext.Current.CancellationToken));
    }

    // ---- helpers ---------------------------------------------------------------------------------------

    private sealed class TempDir : IDisposable
    {
        public string Path { get; } = Directory.CreateTempSubdirectory("cfe-tests-").FullName;
        public void Dispose() => Directory.Delete(Path, recursive: true);
    }

    private sealed class NonSeekableStream(Stream inner) : Stream
    {
        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => inner.CanWrite;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, Math.Min(count, 1000)); // short reads
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default) =>
            inner.ReadAsync(buffer[..Math.Min(buffer.Length, 1000)], ct);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default) => inner.WriteAsync(buffer, ct);
    }

    /// <summary>Deterministic read-only stream of <c>size</c> bytes that is never held in memory.</summary>
    private sealed class PatternStream(long size) : Stream
    {
        private long _pos;
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => size;
        public override long Position { get => _pos; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));
        public override int Read(Span<byte> buffer)
        {
            var n = (int)Math.Min(buffer.Length, size - _pos);
            for (var i = 0; i < n; i++) buffer[i] = (byte)((_pos + i) * 31 + ((_pos + i) >> 8));
            _pos += n;
            return n;
        }
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default) => ValueTask.FromResult(Read(buffer.Span));
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class HashingSink(IncrementalHash hash) : Stream
    {
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => hash.AppendData(buffer, offset, count);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        {
            hash.AppendData(buffer.Span);
            return ValueTask.CompletedTask;
        }
    }
}

internal static class GoldenVector
{
    // Pinned after confirming the format against the independent Python implementation in docs/verify_format.py
    // (Python decrypts this exact file; C# decrypts Python-produced files).
    public const string Sha256Hex = "4721B4F02BC9CF42B0974A00C7A6E1E3CD2C7ABFBBB18D01FE6820943356343D";
}