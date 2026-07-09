using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.DataLoad.Csv.Contracts;
using CadsBridge.Infrastructure.DataLoad.Csv.Strategies;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Factories;

public class CsvDataFileSplitterFactory : ICsvDataFileSplitterStrategyFactory
{
    public ICsvDataFileSplitterStrategy GetStrategy(SplitType splitType)
    {
        return splitType switch
        {
            SplitType.None => new CsvDataFileSplitterStrategyNone(),
            SplitType.ByLines => new CsvDataFileSplitterStrategyByLines(),
            SplitType.BySize => new CsvDataFileSplitterStrategyBySize(),
            _ => throw new ArgumentException($"Invalid SplitType specified: {splitType}", nameof(splitType))
        };
    }
}