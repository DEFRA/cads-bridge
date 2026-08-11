using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Core.DataLoad;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Factories;

public class CsvDataFileSplitterFactory(IEnumerable<ICsvDataFileSplitterStrategy> csvDataFileSplitterStrategies) : ICsvDataFileSplitterStrategyFactory
{
    public ICsvDataFileSplitterStrategy GetStrategy(SplitType splitType)
    {
        return csvDataFileSplitterStrategies.FirstOrDefault(s => s.SplitType == splitType)
               ?? throw new ArgumentException($"No strategy found for SplitType: {splitType}", nameof(splitType));
    }
}