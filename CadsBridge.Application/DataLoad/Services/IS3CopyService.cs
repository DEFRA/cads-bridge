using CadsBridge.Application.DataLoad.Jobs;

namespace CadsBridge.Application.DataLoad.Services;

public interface IS3CopyService
{
    Task<bool> ExecAsync(CsvDataFileImportJob job, CancellationToken cancellationToken = default);
}