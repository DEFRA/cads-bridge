namespace CadsBridge.Infrastructure.Locking;

/// <summary>
/// Serialized payload stored inside the S3 lock object. Used to detect staleness
/// and to identify the owning instance.
/// </summary>
public sealed record LockEntry(
    string Owner,
    DateTimeOffset AcquiredAtUtc,
    DateTimeOffset ExpiresAtUtc);