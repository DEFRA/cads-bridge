using CadsBridge.Infrastructure.DataLoad.Csv.Extensions;
using FluentAssertions;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Csv.Extensions;

public class StringExtensionsTests
{
    [Theory]
    [InlineData("import/cts/bulk/FILE.csv", 1, "import/cts/bulk/FILE/FILE-part-0001.csv")]
    [InlineData("import/cts/daily/FILE.csv", 12, "import/cts/daily/FILE/FILE-part-0012.csv")]
    [InlineData("import/FILE.csv", 1, "import/FILE/FILE-part-0001.csv")]
    [InlineData("FILE.csv", 1, "FILE/FILE-part-0001.csv")]
    [InlineData("import/cts/bulk/FILE", 1, "import/cts/bulk/FILE/FILE-part-0001.csv")]
    public void FormatSplitFileTargetKey_ShouldPlacePartsInAFolderNextToTheSourceFile(string sourceKey, int part, string expected)
    {
        sourceKey.FormatSplitFileTargetKey(part).Should().Be(expected);
    }

    [Fact]
    public void FormatSplitFileTargetKey_ShouldDefaultToPartOne()
    {
        "import/cts/bulk/FILE.csv".FormatSplitFileTargetKey().Should().Be("import/cts/bulk/FILE/FILE-part-0001.csv");
    }
}
