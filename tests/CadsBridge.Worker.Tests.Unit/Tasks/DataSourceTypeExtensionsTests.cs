using CadsBridge.Application.DataLoad.Scanning;
using FluentAssertions;

namespace CadsBridge.Worker.Tests.Unit.Tasks;

public class DataSourceTypeExtensionsTests
{
    [Theory]
    [InlineData("cads/cts/bulk/CTSM_UKV_PROD_BULK_1_CT_LOCATIONS_2026-01-01-000000.csv", "import/cts/bulk")]
    [InlineData("cads/cts/daily/CTSM_UKV_PROD_DELTA_1_CT_LOCATIONS_2026-01-01-000000.csv", "import/cts/daily")]
    [InlineData("CADS/CTS/BULK/file.csv", "import/cts/bulk")]
    public void TryResolveDestinationPrefix_ShouldResolve_WhenSourceKeyIsUnderAScanPrefix(string sourceKey, string expected)
    {
        var resolved = DataSourceTypeExtensions.TryResolveDestinationPrefix(sourceKey, out var destinationPrefix);

        resolved.Should().BeTrue();
        destinationPrefix.Should().Be(expected);
    }

    [Theory]
    [InlineData("incoming/file.csv")]
    [InlineData("cads/cts/bulkier/file.csv")]
    [InlineData("cads/cts/bulk")]
    [InlineData("file.csv")]
    [InlineData("")]
    [InlineData(null)]
    public void TryResolveDestinationPrefix_ShouldNotResolve_WhenSourceKeyIsNotUnderAScanPrefix(string? sourceKey)
    {
        var resolved = DataSourceTypeExtensions.TryResolveDestinationPrefix(sourceKey!, out var destinationPrefix);

        resolved.Should().BeFalse();
        destinationPrefix.Should().BeNull();
    }

    [Theory]
    [InlineData("import/cts/bulk", DataSourceType.CtsBulk)]
    [InlineData("import/cts/daily", DataSourceType.CtsDelta)]
    [InlineData("IMPORT/CTS/BULK", DataSourceType.CtsBulk)]
    [InlineData("import/cts/bulk/", DataSourceType.CtsBulk)]
    public void TryResolveDataSourceType_ShouldResolve_WhenDestinationPrefixMatchesADataSourceType(string destinationPrefix, DataSourceType expected)
    {
        var resolved = DataSourceTypeExtensions.TryResolveDataSourceType(destinationPrefix, out var dataSourceType);

        resolved.Should().BeTrue();
        dataSourceType.Should().Be(expected);
    }

    [Theory]
    [InlineData("import/sam/cattle")]
    [InlineData("import/cts")]
    [InlineData("cads/cts/bulk")]
    [InlineData("")]
    [InlineData(null)]
    public void TryResolveDataSourceType_ShouldNotResolve_WhenDestinationPrefixDoesNotMatchADataSourceType(string? destinationPrefix)
    {
        var resolved = DataSourceTypeExtensions.TryResolveDataSourceType(destinationPrefix!, out _);

        resolved.Should().BeFalse();
    }
}