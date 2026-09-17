using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Infrastructure.DataLoad.Sources;
using FluentAssertions;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Sources;

public class DataSourceStrategyFactoryTests
{
    [Theory]
    [InlineData(DataSourceType.CtsBulk)]
    [InlineData(DataSourceType.CtsDelta)]
    public void Create_ShouldReturnCtsStrategy_ForCtsDataSourceTypes(DataSourceType dataSourceType)
    {
        var strategy = DataSourceStrategyFactory.Create(dataSourceType);

        strategy.Should().BeOfType<CtsDataSourceStrategy>();
    }
}