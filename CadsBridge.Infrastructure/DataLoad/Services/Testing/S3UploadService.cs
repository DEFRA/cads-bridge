using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Application.DataLoad.Services.Testing;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Crypto;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using CadsBridge.Infrastructure.DataLoad.Sources;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Factories;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.DataLoad.Services.Testing;

public class S3UploadService<TClient>(
    IS3ClientFactory s3ClientFactory,
    IAesCryptoTransform aesCryptoTransform,
    DataLoadConfiguration config,
    ILogger<S3UploadService<TClient>> logger) : IS3UploadService where TClient : IStorageClient, new()
{
    private readonly S3ClientFactory.ClientInfo _clientInfo = s3ClientFactory.GetClientInfo<TClient>();

    public async Task<UploadDetails> UploadAsync(string key, string data, DataSourceType dataSource, bool overwriteExisting, CancellationToken cancellationToken = default)
    {
        if (!overwriteExisting && CheckIfFileExists(key, cancellationToken))
        {
            throw new ConflictException($"File with key '{key}' already exists and overwriteExisting is set to false.");
        }

        var encryptedStream = await GetEncryptedStream(data, key, dataSource, cancellationToken);

        var uploadDetails = await UploadStreamToS3(encryptedStream, key, cancellationToken);

        return uploadDetails;
    }

    private bool CheckIfFileExists(string key, CancellationToken cancellationToken)
    {
        try
        {
            var metadata = _clientInfo.Client.GetObjectMetadataAsync(_clientInfo.BucketName, key, cancellationToken).GetAwaiter().GetResult();
            return metadata != null;
        }
        catch (Amazon.S3.AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    private async Task<Stream> GetEncryptedStream(string data, string key, DataSourceType dataSource, CancellationToken cancellationToken)
    {
        try
        {
            var salt = config.Salt;
            var password = DataSourceStrategyFactory.Create(dataSource).DeriveDecryptionPassword(key);

            using var inputStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(data));
            var encryptedStream = new MemoryStream();
            encryptedStream.Position = 0;
            await aesCryptoTransform.EncryptStreamAsync(inputStream, encryptedStream, password, salt, cancellationToken: cancellationToken);
            return encryptedStream;
        }
        catch (Exception e)
        {
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.LogError(e, "Error encrypting data for key '{Key}' and data source '{DataSource}'", key, dataSource);
            }
            throw;
        }
    }

    private async Task<UploadDetails> UploadStreamToS3(Stream stream, string key, CancellationToken cancellationToken)
    {
        try
        {
            var streamLength = stream.Length;

            var request = new PutObjectRequest
            {
                BucketName = _clientInfo.BucketName,
                Key = key,
                InputStream = stream,
                ContentType = "application/octet-stream" // Adjust MIME type as needed
            };

            await _clientInfo.Client.PutObjectAsync(request, cancellationToken);
            return new UploadDetails(key, streamLength, _clientInfo.BucketName, DateTimeOffset.UtcNow);
        }
        catch (Exception e)
        {
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.LogError(e, "Error uploading stream to S3 for key '{Key}'", key);
            }
            throw;
        }
    }
}