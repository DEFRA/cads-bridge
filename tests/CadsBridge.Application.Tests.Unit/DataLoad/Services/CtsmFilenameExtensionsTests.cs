using CadsBridge.Application.DataLoad.Services;
using FluentAssertions;

namespace CadsBridge.Application.Tests.Unit.DataLoad.Services;

public class CtsmFilenameExtensionsTests
{
    [Fact]
    public void Validate_SourceKey_returns_expected_password()
    {
        // CTSM_UKV_PROD_BULK_######_CT_REGISTERED_MOVEMENTS_2026-02-22-074603
        //2026-02-22_MOVEMENTS_REGISTERED_CT_######_BULK_PROD_UKV_CTSM
        var ctsmFilename = new CtsmFilename("UKV", "PROD", "BULK","######", "", "CT_REGISTERED_MOVEMENTS", "2026-02-22-074603");
        var expectedResult = "2026-02-22_MOVEMENTS_REGISTERED_CT_######_BULK_PROD_UKV_CTSM";
        var actualResult = ctsmFilename.DerivePassword();

        Assert.Equal(expectedResult, actualResult);
    }

    [Fact]
    public void Validate_TryParse_Returns_Expected_CtsmFilename()
    {
        var result = CtsmFilenameParser.TryParse("CTSM_UKV_PROD_BULK_123456_1_CT_REGISTERED_MOVEMENTS_2026-02-22-074603.csv", out var ctsmFilename);
        ctsmFilename.Should().NotBeNull();
        ctsmFilename.Should().BeOfType<CtsmFilename>();
        result.Should().BeTrue();

        ctsmFilename.App.Should().Be("UKV");
        ctsmFilename.Env.Should().Be("PROD");
        ctsmFilename.Type.Should().Be("BULK");
        ctsmFilename.BatchId.Should().Be("123456");
        ctsmFilename.PartNo.Should().Be("1");
        ctsmFilename.TableName.Should().Be("CT_REGISTERED_MOVEMENTS");
        ctsmFilename.Timestamp.Should().Be("2026-02-22-074603");
    }
}