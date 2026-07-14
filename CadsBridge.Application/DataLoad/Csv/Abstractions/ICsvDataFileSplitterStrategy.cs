using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad.Jobs;

namespace CadsBridge.Application.DataLoad.Csv.Abstractions;

public interface ICsvDataFileSplitterStrategy
{
    SplitType SplitType { get; }
    Task<long> Process(CsvDataFileSplitJob job, CancellationToken cancellationToken);
}