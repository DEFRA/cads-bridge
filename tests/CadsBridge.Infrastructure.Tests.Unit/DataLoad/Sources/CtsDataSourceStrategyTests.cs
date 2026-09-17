using CadsBridge.Infrastructure.DataLoad.Csv.Extensions;
using CadsBridge.Infrastructure.DataLoad.Sources;
using CadsBridge.Infrastructure.DataLoad.Sources.Parsers;
using FluentAssertions;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Sources;

public class CtsDataSourceStrategyTests
{
    private const string ValidBulkFile = "CTSM_CADS_PROD_BULK_ABC_0004_CT_PARTIES_2026-01-01-012345.csv";

    private readonly CtsDataSourceStrategy _sut = new();

    [Fact]
    public void IsFileNameValidForType_ShouldReturnTrue_WhenFilenameParsesAndTypeMatches()
    {
        _sut.IsFileNameValidForType(ValidBulkFile, "BULK").Should().BeTrue();
    }

    [Fact]
    public void IsFileNameValidForType_ShouldBeCaseInsensitive_ForType()
    {
        _sut.IsFileNameValidForType(ValidBulkFile, "bulk").Should().BeTrue();
    }

    [Fact]
    public void IsFileNameValidForType_ShouldReturnFalse_WhenTypeDoesNotMatch()
    {
        _sut.IsFileNameValidForType(ValidBulkFile, "DELTA").Should().BeFalse();
    }

    [Fact]
    public void IsFileNameValidForType_ShouldReturnFalse_WhenFilenameIsNotCtsm()
    {
        _sut.IsFileNameValidForType("not-a-ctsm-file.csv", "BULK").Should().BeFalse();
    }

    [Fact]
    public void DeriveDecryptionPassword_ShouldMatch_CtsmFilenameDerivePassword()
    {
        var expected = CtsmFilenameParser.Parse(ValidBulkFile)!.DerivePassword();

        _sut.DeriveDecryptionPassword(ValidBulkFile).Should().Be(expected);
    }

    [Fact]
    public void DeriveDecryptionPassword_ShouldThrow_WhenFilenameIsNotCtsm()
    {
        var act = () => _sut.DeriveDecryptionPassword("not-a-ctsm-file.csv");

        act.Should().Throw<FormatException>();
    }
}