using Amazon.S3;

namespace CadsBridge.Application.Services;

public interface IAmazonTransferServiceWrapper
{
    Task TransferAsync(
        IAmazonS3 s3,
        Stream inputStream,
        string bucketName,
        string key,
        long partSize = FileImportCopyService.MinPartitionSize,
        string contentType = "text/plain",
        CancellationToken cancellationToken = default);
}