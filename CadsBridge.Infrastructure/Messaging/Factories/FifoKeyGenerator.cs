using System.Security.Cryptography;
using System.Text;

namespace CadsBridge.Infrastructure.Messaging.Factories;

public static class FifoKeyGenerator
{
    public static string GenerateDeduplicationId(
        string bucket,
        string objectKey,
        string etag,
        string environment)
    {
        var raw = $"{bucket}:{objectKey}:{etag}:{environment}";
        var bytes = Encoding.UTF8.GetBytes(raw);
        return Convert.ToHexString(SHA256.HashData(bytes));
    }

    public static string GenerateMessageGroupId(string objectKey, string environment)
    {
        var prefix = objectKey.Contains('/')
            ? objectKey[..objectKey.LastIndexOf('/')]
            : objectKey;

        return $"{prefix}:{environment}";
    }
}
