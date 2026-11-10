using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.Encryption;
using CadsBridge.Infrastructure.DataLoad.Csv.Extensions;
using CadsBridge.Infrastructure.Encryption.Configuration;
using Microsoft.Extensions.Options;
using System.Text;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Strategies;

public abstract class CsvDataFileSplitterStrategy(
    IFileEncryptor fileEncryptor,
    IOptions<EncryptionConfig> encryptionOptions)
{
    protected async Task UploadAsync(
        IAmazonS3 s3,
        string bucketName,
        string key,
        Stream stream,
        string contentType = "text/csv",
        CancellationToken cancellationToken = default)
    {
        ValidateUploadArguments(s3, bucketName, key, stream);

        await using var encryptedStream = encryptionOptions.Value.StorageEncryptionEnabled
            ? await EncryptStreamAsync(stream, cancellationToken)
            : null;

        var uploadStream = encryptedStream ?? stream;

        if (uploadStream.CanSeek)
        {
            uploadStream.Position = 0;
        }

        await s3.PutObjectAsync(
            new PutObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                InputStream = uploadStream,
                ContentType = contentType
            },
            cancellationToken);
    }

    /// <summary>
    /// Uploads a chunk, encrypting it first when storage encryption is enabled.
    /// Encrypted data is binary, so it must be uploaded as a stream.
    /// </summary>
    protected Task UploadChunkAsync(
        IAmazonS3 s3,
        string bucketName,
        string key,
        StringBuilder chunk,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(chunk);

        if (!encryptionOptions.Value.StorageEncryptionEnabled)
        {
            return s3.UploadChunkAsync(
                bucketName,
                key,
                chunk.ToString(),
                cancellationToken: cancellationToken);
        }

        var inputStream = new MemoryStream(
            Encoding.UTF8.GetBytes(chunk.ToString()),
            writable: false);

        return UploadEncryptedStreamAsync(
            s3,
            bucketName,
            key,
            inputStream,
            cancellationToken);
    }

    /// <summary>
    /// Uploads a chunk, encrypting it first when storage encryption is enabled.
    /// Encrypted data is binary, so it must be uploaded as a stream.
    /// </summary>
    protected Task UploadChunkAsync(
        IAmazonS3 s3,
        string bucketName,
        string key,
        Stream stream,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stream);

        return encryptionOptions.Value.StorageEncryptionEnabled
            ? UploadEncryptedStreamAsync(s3, bucketName, key, stream, cancellationToken)
            : s3.UploadChunkAsync(
                bucketName,
                key,
                stream,
                cancellationToken: cancellationToken);
    }

    private async Task UploadEncryptedStreamAsync(
         IAmazonS3 s3,
         string bucketName,
         string key,
         Stream stream,
         CancellationToken cancellationToken)
    {
        await using var encryptedStream =
            await EncryptStreamAsync(stream, cancellationToken);

        await s3.UploadChunkAsync(
            bucketName,
            key,
            encryptedStream,
            cancellationToken: cancellationToken);
    }

    private async Task<MemoryStream> EncryptStreamAsync(
        Stream stream,
        CancellationToken cancellationToken)
    {
        if (!stream.CanSeek)
        {
            throw new InvalidOperationException(
                "The input stream must support seeking when storage encryption is enabled.");
        }

        stream.Position = 0;

        var encryptedStream = new MemoryStream();

        try
        {
            await fileEncryptor.EncryptAsync(
                stream,
                encryptedStream,
                totalBytes: stream.Length,
                cancellationToken: cancellationToken);

            encryptedStream.Position = 0;
            return encryptedStream;
        }
        catch
        {
            await encryptedStream.DisposeAsync();
            throw;
        }
    }

    private static void ValidateUploadArguments(
       IAmazonS3 s3,
       string bucketName,
       string key,
       Stream stream)
    {
        ArgumentNullException.ThrowIfNull(s3);
        ArgumentException.ThrowIfNullOrEmpty(bucketName);
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(stream);
    }
}