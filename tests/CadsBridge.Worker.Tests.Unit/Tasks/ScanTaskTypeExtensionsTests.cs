using CadsBridge.Worker.Tasks;
using FluentAssertions;

namespace CadsBridge.Worker.Tests.Unit.Tasks;

public class ScanTaskTypeExtensionsTests
{
    [Theory]
    [InlineData("cads/cts/bulk/CTSM_UKV_PROD_BULK_1_CT_LOCATIONS_2026-01-01-000000.csv", "import/cts/bulk")]
    [InlineData("cads/cts/daily/CTSM_UKV_PROD_DELTA_1_CT_LOCATIONS_2026-01-01-000000.csv", "import/cts/daily")]
    [InlineData("CADS/CTS/BULK/file.csv", "import/cts/bulk")]
    public void TryResolveDestinationPrefix_ShouldResolve_WhenSourceKeyIsUnderAScanPrefix(string sourceKey, string expected)
    {
        var resolved = ScanTaskTypeExtensions.TryResolveDestinationPrefix(sourceKey, out var destinationPrefix);

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
        var resolved = ScanTaskTypeExtensions.TryResolveDestinationPrefix(sourceKey!, out var destinationPrefix);

        resolved.Should().BeFalse();
        destinationPrefix.Should().BeNull();
    }
}