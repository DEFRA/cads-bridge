using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.DataLoad.Csv.Factories;
using CadsBridge.Infrastructure.DataLoad.Csv.Strategies;
using FluentAssertions;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Csv.Factories;

public class CsvDataFileSplitterFactoryTests
{
    private readonly CsvDataFileSplitterFactory _sut = new();

    [Fact]
    public void GetStrategy_ReturnsNoneStrategy_ForSplitTypeNone() =>
        _sut.GetStrategy(SplitType.None).Should().BeOfType<CsvDataFileSplitterStrategyNone>();

    [Fact]
    public void GetStrategy_ReturnsByLinesStrategy_ForSplitTypeByLines() =>
        _sut.GetStrategy(SplitType.ByLines).Should().BeOfType<CsvDataFileSplitterStrategyByLines>();

    [Fact]
    public void GetStrategy_ReturnsBySizeStrategy_ForSplitTypeBySize() =>
        _sut.GetStrategy(SplitType.BySize).Should().BeOfType<CsvDataFileSplitterStrategyBySize>();

    [Fact]
    public void GetStrategy_Throws_ForUnmappedSplitType()
    {
        var act = () => _sut.GetStrategy((SplitType)999);

        act.Should().Throw<ArgumentException>();
    }
}