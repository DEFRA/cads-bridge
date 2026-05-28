using Amazon.S3;

namespace CadsBridge.Application.Services;

public interface IFileSplitter
{
    Task SplitFileBySizeAsync(
        IAmazonS3 s3,
        string bucketName,
        string sourceKey,
        string destinationPrefix,
        int chunkSizeMB,
        CancellationToken cancellationToken = default);

    Task SplitFileByLineAsync(
        IAmazonS3 s3,
        string bucketName,
        string sourceKey,
        string destinationPrefix,
        int linesPerChunk,
        CancellationToken cancellationToken = default);
}