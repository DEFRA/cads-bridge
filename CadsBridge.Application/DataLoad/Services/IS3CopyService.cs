using CadsBridge.Application.DataLoad.Jobs;

namespace CadsBridge.Application.DataLoad.Services;

public interface IS3CopyService
{
    Task<long> ExecAsync(CsvDataFileImportJob job, CancellationToken cancellationToken = default);
}