using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Factories;
using System.Runtime.CompilerServices;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class S3FileDiscoveryService<TClient>(IS3ClientFactory s3ClientFactory) : IFileDiscoveryService where TClient : IStorageClient, new()
{
    public async Task<List<string>> GetFileNames(CancellationToken cancellationToken)
    {
        var clientInfo = s3ClientFactory.GetClientInfo<TClient>();
        var result = await ListObjectKeys(clientInfo, cancellationToken).ToListAsync(cancellationToken);
        return result;
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