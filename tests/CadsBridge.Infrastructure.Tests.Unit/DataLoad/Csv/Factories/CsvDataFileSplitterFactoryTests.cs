using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Core.DataLoad;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using CadsBridge.Infrastructure.DataLoad.Csv.Factories;
using CadsBridge.Infrastructure.DataLoad.Csv.Strategies;
using CadsBridge.Infrastructure.Storage.Abstractions;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Csv.Factories;

public class CsvDataFileSplitterFactoryTests
{
    private readonly CsvDataFileSplitterFactory _sut = new(CreateStrategies());

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

    private static ICsvDataFileSplitterStrategy[] CreateStrategies()
    {
        var config = new DataLoadConfiguration { SplitValue = 10000 };

        return
        [
            new CsvDataFileSplitterStrategyNone(Mock.Of<IS3ClientFactory>(), Mock.Of<ILogger<CsvDataFileSplitterStrategyNone>>()),
            new CsvDataFileSplitterStrategyByLines(Mock.Of<IS3ClientFactory>(), config, Mock.Of<ILogger<CsvDataFileSplitterStrategyByLines>>()),
            new CsvDataFileSplitterStrategyBySize(Mock.Of<IS3ClientFactory>(), config, Mock.Of<ILogger<CsvDataFileSplitterStrategyBySize>>())
        ];
    }
}