using System.Security.Cryptography;
using System.Text;

namespace CadsBridge.Core.Ids;

public static class DeterministicGuid
{
    // A fixed application namespace UUID (generate once, never change)
    private static readonly Guid Namespace = new("6ba7b810-9dad-11d1-80b4-00c04fd430c8"); // RFC 4122 URL namespace

    /// <summary>
    /// Produces a deterministic UUID v5 (RFC 4122) from the given input string.
    /// The same input always produces the same GUID.
    /// </summary>
    public static Guid From(string input)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(input);

        Span<byte> namespaceBytes = stackalloc byte[16];
        Namespace.TryWriteBytes(namespaceBytes);

        var inputBytes = Encoding.UTF8.GetBytes(input);
        var combined = new byte[16 + inputBytes.Length];
        namespaceBytes.CopyTo(combined);
        inputBytes.CopyTo(combined, 16);

        Span<byte> hash = stackalloc byte[20]; // SHA-1 = 20 bytes
        SHA1.HashData(combined, hash);

        // Set version 5 (0101) in bits 4–7 of byte 6
        hash[6] = (byte)((hash[6] & 0x0F) | 0x50);
        // Set variant (10xx) in bits 6–7 of byte 8
        hash[8] = (byte)((hash[8] & 0x3F) | 0x80);

        return new Guid(hash[..16]);
    }
}