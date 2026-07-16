namespace CadsBridge.Worker.Tasks;

public interface ITask
{
    Task RunAsync(CancellationToken cancellationToken);
}