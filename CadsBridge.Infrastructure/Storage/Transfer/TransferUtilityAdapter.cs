using Amazon.S3;
using Amazon.S3.Transfer;
using CadsBridge.Application.Storage.Transfer;

namespace CadsBridge.Infrastructure.Storage.Transfer;

public class TransferUtilityAdapter(IAmazonS3 s3) : ITransferUtilityAdapter
{
    private readonly IAmazonS3 _amazonS3 = s3;

    public Task UploadAsync(TransferUtilityUploadRequest request, CancellationToken cancellationToken)
    {
        var util = new TransferUtility(_amazonS3);
        return util.UploadAsync(request, cancellationToken);
    }
}