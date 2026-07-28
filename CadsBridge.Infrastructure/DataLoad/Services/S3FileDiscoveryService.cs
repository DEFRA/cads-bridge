using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Core.ApiClients;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Factories;
using System.Runtime.CompilerServices;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class S3FileDiscoveryService<TClient>(IS3ClientFactory s3ClientFactory, IFileImportApiService fileImportApiService) : IFileDiscoveryService where TClient : IStorageClient, new()
{
    public async Task<List<string>> GetFileNames(CancellationToken cancellationToken)
    {
        var clientInfo = s3ClientFactory.GetClientInfo<TClient>();
        var result = await ListObjectKeys(clientInfo, cancellationToken).ToListAsync(cancellationToken);
        return result;
    }

    public async Task<bool> IsFileValid(string fileName, CancellationToken cancellationToken)
    {
        var existingFile = await fileImportApiService.GetByFileNameIfExists(fileName, cancellationToken);
        if (existingFile is null)
        {
            return true;
        }

        if (existingFile.ImportStatus == FileImportStatus.Failed)
        {
            // Need to reset the file import status to allow reprocessing
            // and report it as valid for processing as exisitng file entry
            // TODO: we need to check the amount of attempts made at reprocessing the and ignore if it has exceeded the max attempts allowed
            await fileImportApiService.MarkReset(existingFile.Id, cancellationToken);
            return true;
        }
        return false;
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