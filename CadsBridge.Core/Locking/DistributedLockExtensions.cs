using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace CadsBridge.Core.Locking;

public static class DistributedLockExtensions
{
    /// <summary>
    /// Runs <paramref name="work"/> while periodically renewing the lease on an already-acquired lock.
    /// If the lease is lost the token passed to the work is cancelled. Returns true when the work ran
    /// to completion with the lock still held, false when the lock was lost.
    /// </summary>
    public static async Task<bool> RunWithRenewalAsync(
        this IDistributedLock distributedLock,
        string lockName,
        ILogger logger,
        Func<CancellationToken, Task> work,
        CancellationToken cancellationToken)
    {
        var lease = distributedLock.LeaseDuration;
        if (lease <= TimeSpan.Zero)
        {
            await work(cancellationToken);
            return true;
        }

        using var workCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var renewCts = new CancellationTokenSource();
        var lockLost = false;

        var renewTask = RenewLoopAsync();

        try
        {
            await work(workCts.Token);
            return !lockLost;
        }
        catch (OperationCanceledException) when (lockLost && !cancellationToken.IsCancellationRequested)
        {
            return false;
        }
        finally
        {
            await renewCts.CancelAsync();
            await renewTask;
        }

        async Task RenewLoopAsync()
        {
            using var timer = new PeriodicTimer(lease / 3);
            var sinceRenewal = Stopwatch.StartNew();
            try
            {
                while (await timer.WaitForNextTickAsync(renewCts.Token))
                {
                    try
                    {
                        if (await distributedLock.TryRenewAsync(lockName, renewCts.Token))
                        {
                            sinceRenewal.Restart();
                            continue;
                        }

                        logger.LogWarning("Lease on distributed lock {LockName} was lost; cancelling work", lockName);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        if (sinceRenewal.Elapsed < lease)
                        {
                            logger.LogWarning(ex, "Failed to renew distributed lock {LockName}; will retry", lockName);
                            continue;
                        }

                        logger.LogError(ex, "Could not renew distributed lock {LockName} before the lease elapsed; cancelling work", lockName);
                    }

                    lockLost = true;
                    await workCts.CancelAsync();
                    return;
                }
            }
            catch (OperationCanceledException)
            {
                // Work finished or caller cancelled - stop renewing.
            }
        }
    }
}
