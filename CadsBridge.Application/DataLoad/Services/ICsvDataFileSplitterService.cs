using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad.Jobs;

namespace CadsBridge.Application.DataLoad.Services;

public interface ICsvDataFileSplitterService
{
    Task<bool> ExecuteAsync(CsvDataFileSplitJob job, CancellationToken cancellationToken);
}