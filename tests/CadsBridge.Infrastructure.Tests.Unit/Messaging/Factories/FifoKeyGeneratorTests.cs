using CadsBridge.Infrastructure.Messaging.Factories;
using FluentAssertions;

namespace CadsBridge.Infrastructure.Tests.Unit.Messaging.Factories;

public class FifoKeyGeneratorTests
{
    [Fact]
    public void GenerateDeduplicationId_ShouldBeDeterministic()
    {
        var id1 = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "path/to/import-file.dat",
            etag: "etag123",
            importType: "BulkUpload",
            environment: "PreProd");

        var id2 = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "path/to/import-file.dat",
            etag: "etag123",
            importType: "BulkUpload",
            environment: "PreProd");

        id1.Should().Be(id2);
    }

    [Fact]
    public void GenerateDeduplicationId_ShouldChangeWhenAnyInputChanges()
    {
        var baseId = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "path/to/import-file.dat",
            etag: "etag123",
            importType: "BulkUpload",
            environment: "PreProd");

        var changedBucket = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "bucketB",
            objectKey: "path/to/import-file.dat",
            etag: "etag123",
            importType: "BulkUpload",
            environment: "PreProd");

        var changedObjectKey = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "other-path/to/import-file.dat",
            etag: "etag123",
            importType: "BulkUpload",
            environment: "PreProd");

        var changedEtag = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "path/to/import-file.dat",
            etag: "etag999",
            importType: "BulkUpload",
            environment: "PreProd");

        var changedImportType = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "path/to/import-file.dat",
            etag: "etag123",
            importType: "OtherType",
            environment: "PreProd");

        var changedEnvironment = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "path/to/import-file.dat",
            etag: "etag123",
            importType: "BulkUpload",
            environment: "Prod");

        changedBucket.Should().NotBe(baseId);
        changedObjectKey.Should().NotBe(baseId);
        changedEtag.Should().NotBe(baseId);
        changedImportType.Should().NotBe(baseId);
        changedEnvironment.Should().NotBe(baseId);
    }

    [Fact]
    public void GenerateDeduplicationId_ShouldReturnValidHexString()
    {
        var id = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "path/to/import-file.dat",
            etag: "etag123",
            importType: "BulkUpload",
            environment: "PreProd");

        id.Should().NotBeNullOrWhiteSpace();
        id.Length.Should().Be(64); // SHA-256 hex string length
        id.Should().MatchRegex("^[A-F0-9]{64}$");
    }

    [Fact]
    public void GenerateMessageGroupId_ShouldFollowExpectedPattern()
    {
        var groupId = FifoKeyGenerator.GenerateMessageGroupId("BulkUpload", "PreProd");

        groupId.Should().Be("BulkUpload:PreProd");
    }

    [Fact]
    public void GenerateMessageGroupId_ShouldChangeWhenInputsChange()
    {
        var id1 = FifoKeyGenerator.GenerateMessageGroupId("BulkUpload", "PreProd");
        var id2 = FifoKeyGenerator.GenerateMessageGroupId("BulkUpload", "Prod");
        var id3 = FifoKeyGenerator.GenerateMessageGroupId("OtherType", "PreProd");

        id2.Should().NotBe(id1);
        id3.Should().NotBe(id1);
    }
}