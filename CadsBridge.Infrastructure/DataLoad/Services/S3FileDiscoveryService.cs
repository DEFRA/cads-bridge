using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Core.ApiClients;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Factories;
using System.Runtime.CompilerServices;
using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Messages;
using CadsBridge.Application.Messaging.Models;
using CadsBridge.Application.Messaging.Publishers;
using CadsBridge.Core.Correlation;
using CadsBridge.Core.Ids;
using CadsBridge.Infrastructure.Messaging.Factories;
using CadsBridge.Infrastructure.Storage.Configuration;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class S3FileDiscoveryService<TClient>(
    IS3ClientFactory s3ClientFactory,
    IFileImportApiService fileImportApiService,
    IMessagePublisher<CadsBridgeFifoQueueClient> cadsBridgeFifoQueuePublisher,
    StorageConfiguration storageConfiguration
    ) : IFileDiscoveryService where TClient : IStorageClient, new()
{
    readonly S3ClientFactory.ClientInfo clientInfo = s3ClientFactory.GetClientInfo<TClient>();

    public async Task<List<string>> GetFileNames(CancellationToken cancellationToken)
    {
        var result = await ListObjectKeys(clientInfo, cancellationToken).ToListAsync(cancellationToken);
        return result;
    }

    public async Task<bool> IsFileValid(string fileName, CancellationToken cancellationToken)
    {
        var existingFile = await fileImportApiService.GetByFileNameIfExists(fileName, cancellationToken);
        return existingFile is null ||
               (existingFile.ImportStatus == FileImportStatus.Failed &&
                existingFile.FailedAttempts < 3);
    }

    public async Task EnQueueFileImportMessages(IReadOnlyList<string> fileNames, CancellationToken cancellationToken)
    {
        if (fileNames.Count == 0) return;

        var oracleEnvironment = storageConfiguration.External.EnvironmentName;
        var bucketName = clientInfo.BucketName;

        foreach (var fileName in fileNames)
        {
            var metaData = await clientInfo.Client.GetObjectMetadataAsync(bucketName, fileName, cancellationToken);
            var etag = metaData.ETag;
            var dedipId = FifoKeyGenerator.GenerateDeduplicationId(bucketName, fileName, etag, oracleEnvironment);

            var message = new CsvDataFileImportMessage
            {
                Bucket = bucketName,
                CorrelationId = CorrelationIdContext.Value,
                ObjectKey = fileName,
                DiscoveredAtUtc = DateTime.UtcNow,
                Etag = etag,
                Id = DeterministicGuid.From(dedipId),
                OracleEnvironment = oracleEnvironment
            };

            var fifoMetadata = new FifoMessageMetadata(
                FifoKeyGenerator.GenerateMessageGroupId(fileName, oracleEnvironment),
                FifoKeyGenerator.GenerateDeduplicationId(bucketName, fileName, etag, oracleEnvironment),
                CorrelationIdContext.Value);

            await cadsBridgeFifoQueuePublisher.PublishAsync(message, fifoMetadata, cancellationToken);
        }
    }

    private static async IAsyncEnumerable<string> ListObjectKeys(S3ClientFactory.ClientInfo clientInfo, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var request = new ListObjectsV2Request { BucketName = clientInfo.BucketName };
        ListObjectsV2Response? response = null;
        do
        {
            response = await clientInfo.Client.ListObjectsV2Async(request, cancellationToken: cancellationToken);
            request.ContinuationToken = response.NextContinuationToken;
            foreach (var key in response.S3Objects.Select(o => o.Key))
            {
                yield return key;
            }
        } while (response.IsTruncated == true);
    }
}