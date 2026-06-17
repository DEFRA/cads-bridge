using CadsBridge.Core.Crypto;
using System.Text;

namespace CadsBridge.Testing.Support.Utilities.Aws;

public static class EncryptionExtensions
{
    public static async Task<MemoryStream> Encrypt(this string content, string password, string salt, CancellationToken cancellationToken)
    {
        using var unencryptedStream = new MemoryStream(Encoding.UTF8.GetBytes(content));
        var cryptoTransform = new AesCryptoTransform();
        var encryptedStream = new MemoryStream();
        await cryptoTransform.EncryptStreamAsync(unencryptedStream, encryptedStream, password, salt, cancellationToken: cancellationToken);
        encryptedStream.Position = 0;
        return encryptedStream;
    }
}