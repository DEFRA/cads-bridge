using Amazon.S3;

namespace CadsBridge.Infrastructure.Storage;

public interface IAmazonTransferServiceWrapper
{
    Task TransferAsync(
        IAmazonS3 s3,
        Stream inputStream,
        string bucketName,
        string key,
        long partSize,
        string contentType = "text/plain",
        CancellationToken cancellationToken = default);
}