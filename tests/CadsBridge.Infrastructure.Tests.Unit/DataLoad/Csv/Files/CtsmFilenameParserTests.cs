using CadsBridge.Infrastructure.DataLoad.Csv.Files;
using FluentAssertions;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Csv.Files;

public class CtsmFilenameParserTests
{
    [Fact]
    public void TryParse_ShouldReturnTrue_ForPattern1WithPartNumber()
    {
        var success = CtsmFilenameParser.TryParse(
            "CTSM_CADS_PROD_BULK_ABC_0004_CT_PARTIES_2026-01-01-012345", out var result);

        success.Should().BeTrue();
        result.Should().NotBeNull();
        result.App.Should().Be("CADS");
        result.Env.Should().Be("PROD");
        result.Type.Should().Be("BULK");
        result.BatchId.Should().Be("ABC");
        result.PartNo.Should().Be("0004");
        result.TableName.Should().Be("CT_PARTIES");
        result.Timestamp.Should().Be("2026-01-01-012345");
    }

    [Fact]
    public void TryParse_ShouldReturnTrue_ForPattern2WithoutPartNumber()
    {
        var success = CtsmFilenameParser.TryParse(
            "CTSM_UKV_PROD_BULK_######_CT_REGISTERED_ANIMALS_2026-02-22-074603.csv", out var result);

        success.Should().BeTrue();
        result.Should().NotBeNull();
        result.App.Should().Be("UKV");
        result.Env.Should().Be("PROD");
        result.Type.Should().Be("BULK");
        result.BatchId.Should().Be("######");
        result.PartNo.Should().BeNull();
        result.TableName.Should().Be("CT_REGISTERED_ANIMALS");
        result.Timestamp.Should().Be("2026-02-22-074603");
    }

    [Theory]
    [InlineData("invalid-filename.csv")]
    [InlineData("not_even_close")]
    [InlineData("CTSM_ONLY_A_FEW_PARTS")]
    public void TryParse_ShouldReturnFalse_ForInvalidFilenames(string filename)
    {
        var success = CtsmFilenameParser.TryParse(filename, out var result);

        success.Should().BeFalse();
        result.Should().BeNull();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void TryParse_ShouldReturnFalse_ForNullOrWhitespaceFilenames(string? filename)
    {
        var success = CtsmFilenameParser.TryParse(filename!, out var result);

        success.Should().BeFalse();
        result.Should().BeNull();
    }

    [Fact]
    public void Parse_ShouldReturnParsedResult_ForValidFilename()
    {
        var result = CtsmFilenameParser.Parse(
            "CTSM_CADS_PROD_DELTA_XYZ_0001_CT_ANIMALS_2026-07-31-120000");

        result.Should().NotBeNull();
        result.Type.Should().Be("DELTA");
    }

    [Fact]
    public void Parse_ShouldThrowFormatException_ForInvalidFilename()
    {
        var act = () => CtsmFilenameParser.Parse("invalid-filename.csv");

        act.Should().Throw<FormatException>()
            .WithMessage("Invalid CTSM filename format: invalid-filename.csv");
    }
}