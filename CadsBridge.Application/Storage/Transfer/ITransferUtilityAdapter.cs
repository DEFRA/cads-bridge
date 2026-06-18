using Amazon.S3;
using Amazon.S3.Transfer;

namespace CadsBridge.Application.Storage.Transfer;

public interface ITransferUtilityAdapter
{
    Task UploadAsync(IAmazonS3 s3Client, TransferUtilityUploadRequest request, CancellationToken cancellationToken);
}