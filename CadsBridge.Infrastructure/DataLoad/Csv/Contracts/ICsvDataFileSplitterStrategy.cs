using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.Storage.Factories;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Contracts;

public interface ICsvDataFileSplitterStrategy
{
    SplitType SplitType { get; }
    Task Process(CsvDataFileSplitJob job, S3ClientFactory.ClientInfo internalS3Info, CancellationToken cancellationToken);
}