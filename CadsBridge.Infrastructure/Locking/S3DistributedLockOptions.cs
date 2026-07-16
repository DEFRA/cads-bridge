namespace CadsBridge.Infrastructure.Locking;

/// <summary>
/// Configuration for the S3-backed distributed lock.
/// Bound from the "DistributedLock" configuration section.
/// </summary>
public record S3DistributedLockOptions
{
    /// <summary>
    /// Key prefix (pseudo-folder) under which lock objects are stored in the internal bucket.
    /// </summary>
    public string KeyPrefix { get; init; } = "locks";

    /// <summary>
    /// Maximum time a lock is considered valid before it becomes stale and can be taken over
    /// by another instance. Defaults to 5 minutes.
    /// </summary>
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Number of times acquisition will retry when the lock object is created and then released
    /// by a competing instance between our conditional create and staleness read.
    /// </summary>
    public int MaxAcquireAttempts { get; init; } = 3;
}