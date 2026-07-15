using Amazon.S3.Transfer;
using CadsBridge.Application.DataLoad.Services;

namespace CadsBridge.Testing.Support.TestDoubles.Storage;

public class FakeS3fileMetaDataService : IS3FileMetaDataService
{
    public readonly List<TransferUtilityUploadRequest> Uploads = [];

    public Task<long> GetRecordCountAsync(string s3Key, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }
}