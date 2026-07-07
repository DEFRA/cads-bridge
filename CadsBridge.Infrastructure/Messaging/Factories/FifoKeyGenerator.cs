using System.Security.Cryptography;
using System.Text;

namespace CadsBridge.Infrastructure.Messaging.Factories;

public static class FifoKeyGenerator
{
    public static string GenerateDeduplicationId(
        string bucket,
        string objectKey,
        string etag,
        string importType,
        string environment)
    {
        var raw = $"{bucket}:{objectKey}:{etag}:{importType}:{environment}";
        var bytes = Encoding.UTF8.GetBytes(raw);
        return Convert.ToHexString(SHA256.HashData(bytes));
    }

    public static string GenerateMessageGroupId(string importType, string environment)
        => $"{importType}:{environment}";
}