using Amazon.S3;
using Amazon.S3.Transfer;
using CadsBridge.Application.Storage.Transfer;

namespace CadsBridge.Testing.Support.TestDoubles.Storage;

public class FakeTransferUtilityAdapter : ITransferUtilityAdapter
{
    public readonly List<TransferUtilityUploadRequest> Uploads = [];

    public Task UploadAsync(IAmazonS3 s3Client, TransferUtilityUploadRequest request, CancellationToken token)
    {
        Uploads.Add(request);
        return Task.CompletedTask;
    }
}