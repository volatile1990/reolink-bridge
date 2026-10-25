using System.Diagnostics;

namespace Neolink.Rtsp;

/// <summary>A single budget covers lock admission and the write. Synchronous
/// socket writes need neither a linked cancellation source nor a deadline timer.</summary>
internal static class RtspWriteGuard
{
    internal static async Task WriteAsync(Stream stream, SemaphoreSlim gate, ReadOnlyMemory<byte> data,
        TimeSpan timeout, Action abort, CancellationToken ct)
    {
        long started = Stopwatch.GetTimestamp();
        bool entered = gate.Wait(0, ct);
        if (!entered)
        {
            TimeSpan admission = timeout - Stopwatch.GetElapsedTime(started);
            if (admission > TimeSpan.Zero)
                entered = await gate.WaitAsync(admission, ct).ConfigureAwait(false);
            if (!entered)
            {
                abort();
                throw new TimeoutException("RTSP write admission timed out");
            }
        }
        try
        {
            TimeSpan remaining = timeout - Stopwatch.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero)
            {
                abort();
                throw new TimeoutException("RTSP write budget exhausted");
            }
            ValueTask operation = stream.WriteAsync(data, ct);
            if (operation.IsCompleted)
            {
                await operation.ConfigureAwait(false);
                return;
            }
            // The operation itself owns the caller's cancellation token. Do not
            // race that cancellation with another wrapper that could release the
            // lock before the underlying write actually finishes.
            Task pending = operation.AsTask();
            remaining = timeout - Stopwatch.GetElapsedTime(started);
            if (remaining < TimeSpan.Zero) remaining = TimeSpan.Zero;
            try { await pending.WaitAsync(remaining).ConfigureAwait(false); }
            catch (TimeoutException)
            {
                // Close the socket before releasing the lock: no later writer
                // can overlap the timed-out native send. Retain/observe pending
                // completion without extending the already expired budget.
                abort();
                _ = pending.ContinueWith(static t => _ = t.Exception,
                    CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted |
                    TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                throw;
            }
        }
        finally { gate.Release(); }
    }
}
