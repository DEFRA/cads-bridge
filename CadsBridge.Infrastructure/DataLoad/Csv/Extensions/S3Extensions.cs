using System.Text;
using Amazon.S3;
using Amazon.S3.Model;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Extensions;

public static class S3Extensions
{
    extension(IAmazonS3 s3)
    {
        public async Task UploadChunkAsync(
            string bucketName,
            string key,
            Stream stream,
            string contentType = "text/csv",
            CancellationToken cancellationToken = default)
        {
            stream.Position = 0; // Reset stream position before upload

            await s3.PutObjectAsync(
                new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    InputStream = stream,
                    ContentType = contentType
                },
                cancellationToken);
        }

        public async Task UploadChunkAsync(
            string bucketName,
            string key,
            string content,
            string contentType = "text/csv",
            CancellationToken cancellationToken = default)
        {
            await using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(content));

            await s3.UploadChunkAsync(
                bucketName,
                key,
                inputStream,
                contentType,
                cancellationToken);
        }
    }

}