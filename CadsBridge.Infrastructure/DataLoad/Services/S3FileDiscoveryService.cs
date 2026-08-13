using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Messages;
using CadsBridge.Application.Messaging.Models;
using CadsBridge.Application.Messaging.Publishers;
using CadsBridge.Core.ApiClients;
using CadsBridge.Core.Ids;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.Messaging.Factories;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Configuration;
using CadsBridge.Infrastructure.Storage.Factories;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class S3FileDiscoveryService<TClient>(
    IS3ClientFactory s3ClientFactory,
    IFileImportApiService fileImportApiService,
    IMessagePublisher<CadsBridgeFifoQueueClient> cadsBridgeFifoQueuePublisher,
    StorageConfiguration storageConfiguration
    ) : IFileDiscoveryService where TClient : IStorageClient, new()
{
    private const int MaxFailedAttempts = 3;
    private const string UnknownTableName = "UNKNOWN";

    private readonly S3ClientFactory.ClientInfo _clientInfo = s3ClientFactory.GetClientInfo<TClient>();

    public async Task<List<string>> GetFileNames(string? prefix = null, CancellationToken cancellationToken = default)
    {
        var result = await ListObjectKeys(_clientInfo, prefix, cancellationToken);
        return [.. result];
    }

    public async Task<bool> IsFileValid(string fileName, CancellationToken cancellationToken)
    {
        var existingFile = await fileImportApiService.GetByFileNameIfExists(fileName, cancellationToken);
        return existingFile is null ||
               (existingFile.ImportStatus == FileImportStatus.Failed &&
                existingFile.DestinationTableName != UnknownTableName &&
                existingFile.FailedAttempts < MaxFailedAttempts);
    }

    public async Task EnQueueFileImportMessages(IReadOnlyList<string> objectKeys, CancellationToken cancellationToken)
    {
        if (objectKeys.Count == 0) return;

        var oracleEnvironment = storageConfiguration.External.EnvironmentName;
        var bucketName = _clientInfo.BucketName;

        foreach (var objectKey in objectKeys)
        {
            // Create a unique correlation id per message queued
            var correlationId = Guid.NewGuid().ToString();
            var metaData = await _clientInfo.Client.GetObjectMetadataAsync(bucketName, objectKey, cancellationToken);
            var etag = metaData.ETag;
            var dedipId = FifoKeyGenerator.GenerateDeduplicationId(bucketName, objectKey, etag, oracleEnvironment);

            var message = new CsvDataFileImportMessage
            {
                Bucket = bucketName,
                CorrelationId = correlationId,
                ObjectKey = objectKey,
                DiscoveredAtUtc = DateTime.UtcNow,
                Etag = etag,
                Id = DeterministicGuid.From(dedipId),
                OracleEnvironment = oracleEnvironment
            };

            var fifoMetadata = new FifoMessageMetadata(
                FifoKeyGenerator.GenerateMessageGroupId(objectKey, oracleEnvironment),
                FifoKeyGenerator.GenerateDeduplicationId(bucketName, objectKey, etag, oracleEnvironment),
                correlationId);

            await cadsBridgeFifoQueuePublisher.PublishAsync(message, fifoMetadata, cancellationToken);
        }
    }

    private static async Task<IEnumerable<string>> ListObjectKeys(S3ClientFactory.ClientInfo clientInfo, string? prefix, CancellationToken cancellationToken = default)
    {
        var keys = new List<string>();

        if (!string.IsNullOrEmpty(prefix))
        {
            prefix = prefix.EndsWith('/') ? prefix : prefix + "/";
        }

        var request = new ListObjectsV2Request
        {
            BucketName = clientInfo.BucketName,
            Prefix = prefix
        };

        ListObjectsV2Response? response = null;
        do
        {
            response = await clientInfo.Client.ListObjectsV2Async(request, cancellationToken: cancellationToken);

            if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
            {
                throw new InvalidOperationException(
                    $"Failed to list objects in bucket {clientInfo.BucketName} with prefix '{prefix}'. StatusCode: {response.HttpStatusCode}");
            }

            if (response.S3Objects is { Count: > 0 })
            {
                keys.AddRange(response.S3Objects.Select(o => o.Key));
            }

            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated.GetValueOrDefault());

        return keys;
    }
}