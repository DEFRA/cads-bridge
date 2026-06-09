using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Core.Storage.Abstractions;
using CadsBridge.Core.Storage.Clients;
using CadsBridge.Core.Storage.Factories;
using Moq;

namespace CadsBridge.Testing.Support.Utilities.Aws;

public static class S3MockBuilder
{
    private const string ExternalBucketName = "external-bucket";
    private const string InternalBucketName = "internal-bucket";

    public static (Mock<IAmazonS3> S3, Mock<IS3ClientFactory> clientFactory, Dictionary<string, string> UploadedObjects) Create(string sourceContent)
    {
        var (s3, s3ClientFactory, uploadedObjects) = Create();

        s3.Setup(client => client.GetObjectMetadataAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new GetObjectMetadataResponse
            {
                ContentLength = Encoding.UTF8.GetByteCount(sourceContent)
            });

        s3.Setup(client => client.GetObjectAsync(
                It.IsAny<GetObjectRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new GetObjectResponse
            {
                ResponseStream = new MemoryStream(Encoding.UTF8.GetBytes(sourceContent))
            });

        s3.Setup(client => client.GetObjectAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new GetObjectResponse
            {
                ResponseStream = new MemoryStream(Encoding.UTF8.GetBytes(sourceContent))
            });

        return (s3, s3ClientFactory, uploadedObjects);
    }


    public static (Mock<IAmazonS3> S3, Mock<IS3ClientFactory> clientFactory, Dictionary<string, string> UploadedObjects) Create()
    {
        var uploadedObjects = new Dictionary<string, string>();

        var s3 = new Mock<IAmazonS3>();

        CapturePutObjectContent(s3, uploadedObjects);

        var s3ClientFactory = new Mock<IS3ClientFactory>();

        s3ClientFactory
            .Setup(x => x.GetClientInfo<ExternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(s3.Object, ExternalBucketName));

        s3ClientFactory
            .Setup(x => x.GetClientInfo<InternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(s3.Object, InternalBucketName));

        return (s3, s3ClientFactory, uploadedObjects);
    }

    public static (Mock<IAmazonS3> S3, Dictionary<string, string> UploadedObjects) CreateCapturingUploads()
    {
        var uploadedObjects = new Dictionary<string, string>();
        var s3 = new Mock<IAmazonS3>();

        CapturePutObjectContent(s3, uploadedObjects);

        return (s3, uploadedObjects);
    }

    private static void CapturePutObjectContent(
        Mock<IAmazonS3> s3,
        Dictionary<string, string> uploadedObjects)
    {
        s3.Setup(client => client.PutObjectAsync(
                It.IsAny<PutObjectRequest>(),
                It.IsAny<CancellationToken>()))
            .Callback<PutObjectRequest, CancellationToken>((request, _) =>
            {
                using var reader = new StreamReader(
                    request.InputStream,
                    Encoding.UTF8,
                    detectEncodingFromByteOrderMarks: true,
                    leaveOpen: true);

                uploadedObjects[request.Key] = reader.ReadToEnd();
            })
            .ReturnsAsync(new PutObjectResponse());
    }
}