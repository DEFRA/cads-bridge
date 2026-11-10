using CadsBridge.Core.DataLoad;

namespace CadsBridge.Application.DataLoad.Csv.Abstractions;

public interface ICsvDataFileSplitterStrategyFactory
{
    ICsvDataFileSplitterStrategy GetStrategy(SplitType splitType);
}