namespace CadsBridge.Core.Locking;

public interface IDistributedLock
{
    Task<bool> TryAcquireAsync(string lockName, CancellationToken cancellationToken = default);
    Task ReleaseAsync(string lockName, CancellationToken cancellationToken = default);
}