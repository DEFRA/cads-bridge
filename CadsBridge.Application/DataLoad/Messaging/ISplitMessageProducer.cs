using CadsBridge.Application.DataLoad.Jobs;

namespace CadsBridge.Application.DataLoad.Messaging;

public interface ISplitMessageProducer
{
    ValueTask SendAsync(CsvDataFileSplitJob historicDataFileSplitJob, CancellationToken cancellationToken = default);
}