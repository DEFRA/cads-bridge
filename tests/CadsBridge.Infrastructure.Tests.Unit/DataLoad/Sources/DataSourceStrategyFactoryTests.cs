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

    [Fact]
    public void Create_ShouldThrowNotSupportedException_ForUnknownDataSourceType()
    {
        Action act = () => DataSourceStrategyFactory.Create((DataSourceType)999);

        act.Should().Throw<NotSupportedException>()
            .WithMessage("No data source strategy is registered for data source type '999'.");
    }
}