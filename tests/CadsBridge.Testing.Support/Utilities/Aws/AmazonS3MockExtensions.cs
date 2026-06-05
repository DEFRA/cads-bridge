using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Core.Crypto;
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
        CancellationToken cancellationToken)
    {
        var inputData = @"test stream data for encryption
                            second line
                            third line";
        using var unencryptedStream = new MemoryStream(Encoding.UTF8.GetBytes(inputData));
        var cryptoTransform = new AesCryptoTransform();
        var encryptedStream = new MemoryStream();
        await cryptoTransform.EncryptStreamAsync(unencryptedStream, encryptedStream, password, salt, cancellationToken: cancellationToken);
        encryptedStream.Position = 0;
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