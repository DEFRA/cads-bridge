using Amazon.S3;
using Amazon.S3.Transfer;

namespace CadsBridge.Infrastructure.Storage;

public class AmazonTransferServiceWrapper : IAmazonTransferServiceWrapper
{
    public async Task TransferAsync(
        IAmazonS3 s3,
        Stream inputStream,
        string bucketName,
        string key,
        long partSize, // 5 MB minimum for multipart
        string contentType = "text/plain",
        CancellationToken cancellationToken = default)
    {
        var transferUtility = new TransferUtility(s3);

        var request = new TransferUtilityUploadRequest
        {
            InputStream = inputStream,
            BucketName = bucketName,
            Key = key,
            StorageClass = S3StorageClass.Standard,
            PartSize = partSize,
            AutoCloseStream = true,
            ContentType = contentType
        };

        await transferUtility.UploadAsync(request, cancellationToken);
    }
}