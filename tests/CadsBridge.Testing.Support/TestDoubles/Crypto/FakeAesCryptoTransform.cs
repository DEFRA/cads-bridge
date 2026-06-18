using CadsBridge.Infrastructure.Crypto;
using System.Text;

namespace CadsBridge.Testing.Support.TestDoubles.Crypto;

public class FakeAesCryptoTransform(string content) : IAesCryptoTransform
{
    private readonly string _content = content;

    public async Task DecryptStreamAsync(
        Stream input,
        Stream output,
        string password,
        string salt,
        long? totalBytes,
        ProgressCallback? callback,
        CancellationToken token)
    {
        var bytes = Encoding.UTF8.GetBytes(_content);
        await output.WriteAsync(bytes, token);
        output.Position = 0;
    }

    // Other interface members can throw NotImplementedException
    public Task EncryptStreamAsync(Stream inputStream, Stream outputStream, string password, string salt, long? totalBytes = null, ProgressCallback? progressCallback = null, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }
}