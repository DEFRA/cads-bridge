using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.FileSystem;
using CadsBridge.Infrastructure.DataLoad.Services;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Factories;
using CadsBridge.Testing.Support.TestDoubles.FileSystem;
using CadsBridge.Testing.Support.TestDoubles.Storage;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Text;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Services;

public class FileSystemToS3CopyServiceTests
{
    private const string BucketName = "test-bucket";

    private readonly FakeFileSystem _fileSystem = new();

    [Fact]
    public async Task Uploads_file_when_source_exists()
    {
        const string targetKey = "data-seed/001_seed.sql";
        const string fileContent = "FILE CONTENT;";
        const string sourcePath = "abc/001_seed.sql";

        _fileSystem.AddFile(sourcePath, fileContent);

        var s3 = new FakeS3();
        var sut = CreateSut(s3);

        var request = new DataSeedFileLoadJob(
            JobId: "job-1",
            FileName: sourcePath,
            TargetKey: targetKey);

        var result = await sut.ExecuteAsync(request, CancellationToken.None);

        result.Should().BeTrue();

        s3.PutRequests.Should().ContainSingle(r =>
            r.Key == targetKey &&
            r.BucketName == BucketName &&
            r.ContentType == "text/plain");

        s3.UploadedContent[targetKey].Should().Be(fileContent);
    }

    [Fact]
    public async Task Returns_false_when_source_file_missing()
    {
        var s3 = new FakeS3();
        var sut = CreateSut(s3);

        var request = new DataSeedFileLoadJob(
            JobId: "job-1",
            FileName: "abc/missing.sql",
            TargetKey: "data-seed/missing.sql");

        var result = await sut.ExecuteAsync(request, CancellationToken.None);

        result.Should().BeFalse();
        s3.PutRequests.Should().BeEmpty();
    }

    private FileSystemToS3CopyService CreateSut(IAmazonS3 s3)
    {
        var factory = new Mock<IS3ClientFactory>();

        factory.Setup(x => x.GetClientInfo<InternalStorageClient>())
               .Returns(new S3ClientFactory.ClientInfo(s3, BucketName));

        return new FileSystemToS3CopyService(
            factory.Object,
            _fileSystem,
            Mock.Of<ILogger<FileSystemToS3CopyService>>());
    }
}