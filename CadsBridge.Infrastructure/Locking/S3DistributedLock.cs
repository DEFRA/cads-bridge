using System.Collections.Concurrent;
using System.Net;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Core.Locking;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.Locking;

/// <summary>
/// A distributed lock backed by S3 conditional writes.
///
/// Acquisition relies on S3's atomic "create if absent" semantics
/// (<c>If-None-Match: *</c>): only one caller across all instances can create the
/// lock object, so exactly one holder is guaranteed per <c>lockName</c>.
///
/// A lease timestamp is stored inside the object; once the lease expires the lock is
/// considered stale and a competing instance may take it over via an ETag-guarded
/// (<c>If-Match</c>) overwrite. Release is an ETag-guarded delete so an instance can
/// never remove a lease that has already been taken over by another instance.
/// </summary>
public sealed class S3DistributedLock(
    IS3ClientFactory s3ClientFactory,
    S3DistributedLockOptions options,
    TimeProvider timeProvider,
    ILogger<S3DistributedLock> logger) : IDistributedLock
{
    private readonly ConcurrentDictionary<string, HeldLock> _heldLocks = new();

    public async Task<bool> TryAcquireAsync(string lockName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(lockName);

        var (client, bucket) = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        var key = BuildKey(lockName);

        for (var attempt = 0; attempt < options.MaxAcquireAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var now = timeProvider.GetUtcNow();
            var entry = new LockEntry(
                Owner: Guid.NewGuid().ToString("N"),
                AcquiredAtUtc: now,
                ExpiresAtUtc: now.Add(options.LeaseDuration));

            try
            {
                // Atomic create-if-absent: succeeds only when no object exists for this key.
                var etag = await ConditionalPutAsync(client, bucket, key, entry, ifNoneMatch: "*", ifMatch: null, cancellationToken);
                _heldLocks[lockName] = new HeldLock(entry.Owner, etag);
                logger.LogDebug("Acquired distributed lock {LockName}", lockName);
                return true;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                // The lock already exists. Determine whether it is stale and can be taken over.
                var existing = await TryReadAsync(client, bucket, key, cancellationToken);
                if (existing is null)
                {
                    // Released between our create attempt and read - retry the create.
                    continue;
                }

                if (existing.Value.Entry.ExpiresAtUtc > now)
                {
                    logger.LogDebug("Distributed lock {LockName} is held and valid until {Expiry}", lockName, existing.Value.Entry.ExpiresAtUtc);
                    return false;
                }

                if (await TryTakeOverAsync(client, bucket, key, lockName, entry, existing.Value.ETag, cancellationToken))
                {
                    return true;
                }

                // Another instance took over the stale lock first.
                return false;
            }
        }

        logger.LogDebug("Failed to acquire distributed lock {LockName} after {Attempts} attempts", lockName, options.MaxAcquireAttempts);
        return false;
    }

    public async Task ReleaseAsync(string lockName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(lockName);

        if (!_heldLocks.TryRemove(lockName, out var held))
        {
            // We do not hold this lock (e.g. it expired and was taken over) - nothing to do.
            return;
        }

        var (client, bucket) = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        var key = BuildKey(lockName);

        try
        {
            // Conditional delete: only removes the object while it still carries our ETag,
            // preventing deletion of a lease that has since been taken over.
            await client.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucket,
                Key = key,
                IfMatch = held.ETag
            }, cancellationToken);
            logger.LogDebug("Released distributed lock {LockName}", lockName);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode is HttpStatusCode.PreconditionFailed or HttpStatusCode.NotFound)
        {
            // Lease was taken over or already gone - safe to ignore.
            logger.LogDebug("Distributed lock {LockName} was already released or taken over", lockName);
        }
    }

    private async Task<bool> TryTakeOverAsync(
        IAmazonS3 client,
        string bucket,
        string key,
        string lockName,
        LockEntry entry,
        string existingETag,
        CancellationToken cancellationToken)
    {
        try
        {
            var etag = await ConditionalPutAsync(client, bucket, key, entry, ifNoneMatch: null, ifMatch: existingETag, cancellationToken);
            _heldLocks[lockName] = new HeldLock(entry.Owner, etag);
            logger.LogInformation("Took over stale distributed lock {LockName}", lockName);
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
        {
            return false;
        }
    }

    private static async Task<string> ConditionalPutAsync(
        IAmazonS3 client,
        string bucket,
        string key,
        LockEntry entry,
        string? ifNoneMatch,
        string? ifMatch,
        CancellationToken cancellationToken)
    {
        var request = new PutObjectRequest
        {
            BucketName = bucket,
            Key = key,
            ContentBody = JsonSerializer.Serialize(entry),
            ContentType = "application/json",
            IfNoneMatch = ifNoneMatch,
            IfMatch = ifMatch
        };

        var response = await client.PutObjectAsync(request, cancellationToken);
        return response.ETag;
    }

    private static async Task<(LockEntry Entry, string ETag)?> TryReadAsync(
        IAmazonS3 client,
        string bucket,
        string key,
        CancellationToken cancellationToken)
    {
        try
        {
            using var response = await client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket,
                Key = key
            }, cancellationToken);

            await using var stream = response.ResponseStream;
            var entry = await JsonSerializer.DeserializeAsync<LockEntry>(stream, cancellationToken: cancellationToken);
            return entry is null ? null : (entry, response.ETag);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    private string BuildKey(string lockName) => $"{options.KeyPrefix.TrimEnd('/')}/{lockName}.lock";

    private readonly record struct HeldLock(string Owner, string ETag);
}

