using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad;

namespace CadsBridge.Application.DataLoad.Csv.Abstractions;

public interface ICsvDataFileSplitterStrategy
{
    SplitType SplitType { get; }
    Task<long> ProcessAsync(CsvDataFileSplitJob job, CancellationToken cancellationToken);
}