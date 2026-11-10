using CadsBridge.Application.Encryption;

namespace CadsBridge.Application.Extensions;

public static class FileEncryptorExtensions
{
    /// <summary>
    /// Decrypts into a temporary file next to <paramref name="destinationPath"/> and moves it into place only after
    /// the whole file has authenticated. On any failure the temporary file is deleted and the destination is untouched.
    /// </summary>
    public static async Task DecryptToFileAsync(this IFileEncryptor encryptor, Stream input, string destinationPath,
        long? totalBytes = null, Action<int, string>? progress = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(encryptor);
        ArgumentException.ThrowIfNullOrEmpty(destinationPath);

        var tempPath = $"{destinationPath}.{Guid.NewGuid():N}.tmp";
        try
        {
            await using (var temp = new FileStream(tempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None,
                             bufferSize: 81920, FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                await encryptor.DecryptAsync(input, temp, totalBytes, progress, cancellationToken).ConfigureAwait(false);
                await temp.FlushAsync(cancellationToken).ConfigureAwait(false);
            }

            File.Move(tempPath, destinationPath, overwrite: true);
        }
        catch
        {
            try { File.Delete(tempPath); } catch (IOException) { } catch (UnauthorizedAccessException) { }
            throw;
        }
    }

    public static async Task<Stream> EncryptAsync(this IFileEncryptor encryptor, Stream stream,
       CancellationToken cancellationToken)
    {
        stream.Position = 0;

        var encryptedStream = new MemoryStream();

        await encryptor.EncryptAsync(
            stream,
            encryptedStream,
            totalBytes: stream.Length,
            cancellationToken: cancellationToken);

        return encryptedStream;
    }
}