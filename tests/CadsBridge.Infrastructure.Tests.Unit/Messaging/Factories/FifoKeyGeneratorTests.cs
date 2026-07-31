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
            objectKey: "path/abc/import-file.dat",
            etag: "etag123",
            environment: "PreProd");

        var id2 = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "path/abc/import-file.dat",
            etag: "etag123",
            environment: "PreProd");

        id1.Should().Be(id2);
    }

    [Fact]
    public void GenerateDeduplicationId_ShouldChangeWhenAnyInputChanges()
    {
        var baseId = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "path/abc/import-file.dat",
            etag: "etag123",
            environment: "PreProd");

        var changedBucket = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "bucketB",
            objectKey: "path/abc/import-file.dat",
            etag: "etag123",
            environment: "PreProd");

        var changedObjectKey = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "other-path/abc/import-file.dat",
            etag: "etag123",
            environment: "PreProd");

        var changedEtag = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "path/abc/import-file.dat",
            etag: "etag999",
            environment: "PreProd");

        var changedEnvironment = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "path/abc/import-file.dat",
            etag: "etag123",
            environment: "Prod");

        changedBucket.Should().NotBe(baseId);
        changedObjectKey.Should().NotBe(baseId);
        changedEtag.Should().NotBe(baseId);
        changedEnvironment.Should().NotBe(baseId);
    }

    [Fact]
    public void GenerateDeduplicationId_ShouldReturnValidHexString()
    {
        var id = FifoKeyGenerator.GenerateDeduplicationId(
            bucket: "example-bucket",
            objectKey: "path/abc/import-file.dat",
            etag: "etag123",
            environment: "PreProd");

        id.Should().NotBeNullOrWhiteSpace();
        id.Length.Should().Be(64); // SHA-256 hex string length
        id.Should().MatchRegex("^[A-F0-9]{64}$");
    }

    [Fact]
    public void GenerateMessageGroupId_ShouldFollowExpectedPattern()
    {
        var groupId = FifoKeyGenerator.GenerateMessageGroupId("path/abc/import-file.dat", "PreProd");

        groupId.Should().Be("path/abc:PreProd");
    }

    [Fact]
    public void GenerateMessageGroupId_ShouldChangeWhenInputsChange()
    {
        var id1 = FifoKeyGenerator.GenerateMessageGroupId("path/abc/import-file.dat", "PreProd");
        var id2 = FifoKeyGenerator.GenerateMessageGroupId("path/abc/import-file.dat", "Prod");
        var id3 = FifoKeyGenerator.GenerateMessageGroupId("path/def/import-file.dat", "PreProd");

        id2.Should().NotBe(id1);
        id3.Should().NotBe(id1);
    }

    [Fact]
    public void GenerateMessageGroupId_ShouldUseWholeObjectKey_WhenNoSlashPresent()
    {
        var groupId = FifoKeyGenerator.GenerateMessageGroupId("import-file.dat", "PreProd");

        groupId.Should().Be("import-file.dat:PreProd");
    }
}