using CadsBridge.Application.DataLoad.Jobs;

namespace CadsBridge.Application.DataLoad.Csv.Abstractions;

public interface ICsvDataFileSplitterService
{
    Task<long> ExecuteAsync(CsvDataFileSplitJob job, CancellationToken cancellationToken);
}