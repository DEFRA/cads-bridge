namespace CadsBridge.Infrastructure.Crypto;

public delegate void ProgressCallback(int progressPercentage, string status);

public interface IAesCryptoTransform
{
    Task EncryptStreamAsync(Stream inputStream, Stream outputStream, string password, string salt,
        long? totalBytes = null, ProgressCallback? progressCallback = null, CancellationToken cancellationToken = default);

    Task DecryptStreamAsync(Stream inputStream, Stream outputStream, string password, string salt,
        long? totalBytes = null, ProgressCallback? progressCallback = null, CancellationToken cancellationToken = default);
}