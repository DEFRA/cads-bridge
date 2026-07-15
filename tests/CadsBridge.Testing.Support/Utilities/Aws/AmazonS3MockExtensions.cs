using Amazon.S3;
using Amazon.S3.Model;
using Moq;

namespace CadsBridge.Testing.Support.Utilities.Aws;

public static class AmazonS3MockExtensions
{
    public static async Task SetUpEncryptedFileAsync(
        this Mock<IAmazonS3> s3Mock,
        string bucketName,
        string key,
        string password,
        string salt,
        string content,
        CancellationToken cancellationToken)
    {
        using var encryptedStream = await content.Encrypt(password, salt, cancellationToken);
        var buffer = new byte[encryptedStream.Length];
        await encryptedStream.ReadExactlyAsync(buffer, 0, buffer.Length, cancellationToken);

        s3Mock.Setup(x => x.GetObjectAsync(
                bucketName,
                key,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetObjectResponse() { ResponseStream = new MemoryStream(buffer) });

        s3Mock.Setup(x => x.GetObjectMetadataAsync(
                bucketName,
                key,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetObjectMetadataResponse() { ContentLength = encryptedStream.Length });
    }

}