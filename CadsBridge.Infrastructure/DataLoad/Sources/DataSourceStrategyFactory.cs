using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Application.DataLoad.Sources;

namespace CadsBridge.Infrastructure.DataLoad.Sources;

public static class DataSourceStrategyFactory
{
    public static IDataSourceStrategy Create(DataSourceType dataSourceType) => dataSourceType switch
    {
        DataSourceType.CtsBulk or DataSourceType.CtsDelta => new CtsDataSourceStrategy(),
        _ => throw new NotSupportedException(
            $"No data source strategy is registered for data source type '{dataSourceType}'.")
    };
}