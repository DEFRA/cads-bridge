namespace CadsBridge.Infrastructure.Encryption.Configuration;

public class EncryptionConfig
{
    public static readonly string SectionName = "Encryption";

    public required string CurrentKeyId { get; init; }
    public bool StorageEncryptionEnabled { get; init; }
    public required Dictionary<string, string> Keys { get; init; }

    public IReadOnlyDictionary<string, byte[]> GetKeys()
    {
        return Keys.ToDictionary(
            x => x.Key,
            x => Convert.FromBase64String(x.Value));
    }
}