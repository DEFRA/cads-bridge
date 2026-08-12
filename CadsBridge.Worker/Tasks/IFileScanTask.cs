namespace CadsBridge.Worker.Tasks;

public interface IFileScanTask : ITask
{
    Task RunAsync(CancellationToken cancellationToken);
}