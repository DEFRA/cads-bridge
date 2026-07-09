using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Infrastructure.Storage.Factories;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Contracts;

public interface ICsvDataFileSplitterStrategy
{
    Task Process(CsvDataFileSplitJob job, S3ClientFactory.ClientInfo internalS3Info, ILogger logger, CancellationToken cancellationToken);
}