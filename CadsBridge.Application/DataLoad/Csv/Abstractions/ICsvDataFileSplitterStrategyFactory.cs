using CadsBridge.Core.DataLoad.Jobs;

namespace CadsBridge.Application.DataLoad.Csv.Abstractions;

public interface ICsvDataFileSplitterStrategyFactory
{
    ICsvDataFileSplitterStrategy GetStrategy(SplitType splitType);
}