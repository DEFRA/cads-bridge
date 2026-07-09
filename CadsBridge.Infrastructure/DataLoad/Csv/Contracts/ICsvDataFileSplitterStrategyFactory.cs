using CadsBridge.Core.DataLoad.Jobs;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Contracts;

public interface ICsvDataFileSplitterStrategyFactory
{
    ICsvDataFileSplitterStrategy GetStrategy(SplitType splitType);
}