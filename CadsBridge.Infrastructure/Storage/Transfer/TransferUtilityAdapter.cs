using Amazon.S3;
using Amazon.S3.Transfer;
using CadsBridge.Application.Storage.Transfer;

namespace CadsBridge.Infrastructure.Storage.Transfer;

public class TransferUtilityAdapter : ITransferUtilityAdapter
{
    public Task UploadAsync(IAmazonS3 s3Client, TransferUtilityUploadRequest request, CancellationToken cancellationToken)
    {
        var util = new TransferUtility(s3Client);
        return util.UploadAsync(request, cancellationToken);
    }
}