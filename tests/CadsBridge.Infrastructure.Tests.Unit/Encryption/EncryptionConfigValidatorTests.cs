using CadsBridge.Application.Encryption;
using CadsBridge.Infrastructure.Encryption.Configuration;
using CadsBridge.Infrastructure.Encryption.Setup;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Security.Cryptography;
using System.Text;

namespace CadsBridge.Infrastructure.Tests.Unit.Encryption;

public class EncryptionConfigValidatorTests
{
    private const string Placeholder = "Set in cdp-app-config (or for local development using docker-compose.override.yml)";

    private static string Base64Key() => Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));

    private static ServiceProvider Build(bool enabled, string currentKeyId, string? keyValue)
    {
        var settings = new Dictionary<string, string?>
        {
            ["Encryption:CurrentKeyId"] = currentKeyId,
            ["Encryption:StorageEncryptionEnabled"] = enabled.ToString(),
        };
        if (keyValue is not null)
            settings["Encryption:Keys:V1"] = keyValue;

        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        return new ServiceCollection().AddEncryption(configuration).BuildServiceProvider();
    }

    [Fact]
    public void WhenEnabled_AndKeyIsThePlaceholder_ShouldFailOptionsValidation()
    {
        using var provider = Build(enabled: true, "V1", Placeholder);

        var act = () => provider.GetRequiredService<IOptions<EncryptionConfig>>().Value;

        act.Should().Throw<OptionsValidationException>();
    }

    [Fact]
    public void WhenEnabled_AndCurrentKeyIdIsUnknown_ShouldFailOptionsValidation()
    {
        using var provider = Build(enabled: true, "V2", Base64Key());

        var act = () => provider.GetRequiredService<IOptions<EncryptionConfig>>().Value;

        act.Should().Throw<OptionsValidationException>().Which.Failures.Should().ContainSingle().Which.Should().Contain("V2");
    }

    [Fact]
    public void WhenDisabled_AndKeyIsThePlaceholder_ShouldStillResolveTheEncryptor()
    {
        using var provider = Build(enabled: false, "V1", Placeholder);

        var act = () => provider.GetRequiredService<IFileEncryptor>();

        act.Should().NotThrow();
    }

    [Fact]
    public async Task WhenDisabled_UsingTheEncryptor_ShouldThrowAClearError()
    {
        using var provider = Build(enabled: false, "V1", Placeholder);
        var encryptor = provider.GetRequiredService<IFileEncryptor>();

        var act = () => encryptor.EncryptAsync(new MemoryStream([1]), new MemoryStream(), cancellationToken: TestContext.Current.CancellationToken);

        (await act.Should().ThrowAsync<InvalidOperationException>()).Which.Message.Should().Contain("disabled");
    }

    [Fact]
    public async Task WhenEnabled_AndKeysAreValid_ShouldRoundTripThroughTheRegisteredEncryptor()
    {
        using var provider = Build(enabled: true, "V1", Base64Key());
        var encryptor = provider.GetRequiredService<IFileEncryptor>();
        var plaintext = Encoding.UTF8.GetBytes("record_type|col2\nA|B\nT|END");

        using var encrypted = new MemoryStream();
        await encryptor.EncryptAsync(new MemoryStream(plaintext), encrypted, cancellationToken: TestContext.Current.CancellationToken);
        encrypted.Position = 0;

        using var decrypted = new MemoryStream();
        await encryptor.DecryptAsync(encrypted, decrypted, cancellationToken: TestContext.Current.CancellationToken);

        decrypted.ToArray().Should().Equal(plaintext);
    }

    [Fact]
    public void Encryptor_ShouldBeRegisteredAsASingleton()
    {
        using var provider = Build(enabled: true, "V1", Base64Key());

        provider.GetRequiredService<IFileEncryptor>().Should().BeSameAs(provider.GetRequiredService<IFileEncryptor>());
    }
}