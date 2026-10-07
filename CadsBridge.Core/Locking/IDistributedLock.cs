namespace CadsBridge.Core.Locking;

public interface IDistributedLock
{
    TimeSpan LeaseDuration { get; }
    Task<bool> TryAcquireAsync(string lockName, CancellationToken cancellationToken = default);
    Task<bool> TryRenewAsync(string lockName, CancellationToken cancellationToken = default);
    Task ReleaseAsync(string lockName, CancellationToken cancellationToken = default);
}