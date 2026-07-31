using CadsBridge.Core.Ids;
using FluentAssertions;

namespace CadsBridge.Infrastructure.Tests.Unit.Ids;

public class DeterministicGuidTests
{
    [Fact]
    public void From_ShouldBeDeterministic_ForSameInput()
    {
        var guid1 = DeterministicGuid.From("some-deterministic-key");
        var guid2 = DeterministicGuid.From("some-deterministic-key");

        guid1.Should().Be(guid2);
    }

    [Fact]
    public void From_ShouldReturnDifferentGuids_ForDifferentInput()
    {
        var guid1 = DeterministicGuid.From("input-a");
        var guid2 = DeterministicGuid.From("input-b");

        guid1.Should().NotBe(guid2);
    }

    [Fact]
    public void From_ShouldSetVersion8_InGeneratedGuid()
    {
        var guid = DeterministicGuid.From("version-check");
        var bytes = guid.ToByteArray();

        // Guid.ToByteArray() preserves the little-endian layout used when constructing
        // the Guid, so byte index 6 (low byte of the "time_hi_and_version" field) holds
        // the same value as the source hash byte where the version nibble was set.
        var versionNibble = (bytes[6] & 0xF0) >> 4;
        versionNibble.Should().Be(8);
    }

    [Fact]
    public void From_ShouldSetRfc4122Variant_InGeneratedGuid()
    {
        var guid = DeterministicGuid.From("variant-check");
        var bytes = guid.ToByteArray();

        var variantBits = (bytes[8] & 0xC0) >> 6;
        variantBits.Should().Be(2); // binary 10xxxxxx
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void From_ShouldThrowArgumentException_WhenInputIsNullOrWhitespace(string? input)
    {
        var act = () => DeterministicGuid.From(input!);

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void From_ShouldNotReturnEmptyGuid_ForValidInput()
    {
        var guid = DeterministicGuid.From("non-empty-check");

        guid.Should().NotBe(Guid.Empty);
    }
}

