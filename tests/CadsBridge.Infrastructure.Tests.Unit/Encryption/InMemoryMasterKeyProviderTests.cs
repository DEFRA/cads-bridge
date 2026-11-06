using CadsBridge.Infrastructure.Encryption.Configuration;
using CadsBridge.Infrastructure.Encryption.Providers;
using Microsoft.Extensions.Options;

namespace CadsBridge.Infrastructure.Tests.Unit.Encryption;

public class InMemoryMasterKeyProviderTests
{
    private static byte[] Key(byte seed) =>
        [.. Enumerable.Range(0, 32).Select(index => (byte)(seed + index))];

    [Fact]
    public async Task GetCurrentAsync_ReturnsConfiguredCurrentKey()
    {
        var currentKey = Key(1);
        var provider = CreateProvider(
            currentKeyId: "current",
            ("current", currentKey),
            ("previous", Key(2)));

        var result = await provider.GetCurrentAsync(TestContext.Current.CancellationToken);

        Assert.Equal("current", result.Id);
        Assert.Equal(currentKey, result.Key);
    }

    [Fact]
    public async Task GetByIdAsync_ReturnsRequestedKey()
    {
        var requestedKey = Key(2);
        var provider = CreateProvider(
            currentKeyId: "current",
            ("current", Key(1)),
            ("requested", requestedKey));

        var result = await provider.GetByIdAsync("requested", TestContext.Current.CancellationToken);

        Assert.Equal(requestedKey, result);
    }

    [Fact]
    public async Task GetByIdAsync_UnknownKey_ThrowsKeyNotFoundException()
    {
        var provider = CreateProvider(
            currentKeyId: "current",
            ("current", Key(1)));

        await Assert.ThrowsAsync<KeyNotFoundException>(
            async () => await provider.GetByIdAsync(
                "unknown",
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task GetCurrentAsync_ReturnsCopyOfKey()
    {
        var originalKey = Key(1);
        var provider = CreateProvider(
            currentKeyId: "current",
            ("current", originalKey));

        var firstResult = await provider.GetCurrentAsync(TestContext.Current.CancellationToken);
        firstResult.Key[0] = 255;

        var secondResult = await provider.GetCurrentAsync(TestContext.Current.CancellationToken);

        Assert.Equal(originalKey, secondResult.Key);
    }

    [Fact]
    public async Task GetByIdAsync_ReturnsCopyOfKey()
    {
        var originalKey = Key(1);
        var provider = CreateProvider(
            currentKeyId: "current",
            ("current", originalKey));

        var firstResult = await provider.GetByIdAsync("current", TestContext.Current.CancellationToken);
        firstResult[0] = 255;

        var secondResult = await provider.GetByIdAsync("current", TestContext.Current.CancellationToken);

        Assert.Equal(originalKey, secondResult);
    }

    [Fact]
    public async Task Constructor_ClonesInputKeys()
    {
        var inputKey = Key(1);
        var provider = CreateProvider(
            currentKeyId: "current",
            ("current", inputKey));

        inputKey[0] = 255;

        var result = await provider.GetCurrentAsync(TestContext.Current.CancellationToken);

        Assert.NotEqual(255, result.Key[0]);
        Assert.Equal(Key(1), result.Key);
    }

    [Fact]
    public void Constructor_MissingCurrentKey_ThrowsArgumentException()
    {
        var keys = new Dictionary<string, byte[]>
        {
            ["available"] = Key(1)
        };

        var exception = Assert.Throws<ArgumentException>(
            () => new InMemoryMasterKeyProvider("missing", keys));

        Assert.Contains("Current key id is not in the key set.", exception.Message);
    }

    [Fact]
    public void Constructor_EmptyCurrentKeyId_ThrowsArgumentException()
    {
        var keys = new Dictionary<string, byte[]>
        {
            ["current"] = Key(1)
        };

        Assert.Throws<ArgumentException>(
            () => new InMemoryMasterKeyProvider(string.Empty, keys));
    }

    [Fact]
    public void Constructor_NullKeys_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(
            () => new InMemoryMasterKeyProvider(
                "current",
                null!));
    }

    [Fact]
    public void Constructor_KeyWithInvalidLength_ThrowsArgumentException()
    {
        var keys = new Dictionary<string, byte[]>
        {
            ["current"] = new byte[16]
        };

        Assert.Throws<ArgumentException>(
            () => new InMemoryMasterKeyProvider("current", keys));
    }

    [Fact]
    public void Constructor_KeyIdLongerThan32Utf8Bytes_ThrowsArgumentException()
    {
        var keyId = new string('k', 33);
        var keys = new Dictionary<string, byte[]>
        {
            [keyId] = Key(1)
        };

        Assert.Throws<ArgumentException>(
            () => new InMemoryMasterKeyProvider(keyId, keys));
    }

    [Fact]
    public async Task OptionsConstructor_LoadsConfiguredKeys()
    {
        var currentKey = Key(1);
        var previousKey = Key(2);

        var options = Options.Create(new EncryptionConfig
        {
            CurrentKeyId = "current",
            Keys = new Dictionary<string, string>
            {
                ["current"] = Convert.ToBase64String(currentKey),
                ["previous"] = Convert.ToBase64String(previousKey)
            }
        });

        var provider = new InMemoryMasterKeyProvider(options);

        var current = await provider.GetCurrentAsync(TestContext.Current.CancellationToken);
        var previous = await provider.GetByIdAsync("previous", TestContext.Current.CancellationToken);

        Assert.Equal("current", current.Id);
        Assert.Equal(currentKey, current.Key);
        Assert.Equal(previousKey, previous);
    }

    private static InMemoryMasterKeyProvider CreateProvider(
        string currentKeyId,
        params (string Id, byte[] Key)[] keys)
    {
        var keyDictionary = keys.ToDictionary(
            key => key.Id,
            key => key.Key);

        return new InMemoryMasterKeyProvider(
            currentKeyId,
            keyDictionary);
    }
}