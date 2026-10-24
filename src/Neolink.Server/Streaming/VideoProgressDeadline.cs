// Licensed under the GNU Affero General Public License v3.0; see LICENSE.
using System.Diagnostics;
using Neolink.Media;

namespace Neolink.Streaming;

/// <summary>
/// A preview's monotonic video deadline, independent of transport/audio traffic.
/// Cancelling the preview interrupts a partially parsed frame as well as a quiet
/// read. That parser is then discarded; no abandoned reads survive into a retry.
/// </summary>
internal sealed class VideoProgressDeadline : IAsyncDisposable
{
    internal static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(15);
    private readonly CancellationTokenSource _preview;
    private readonly CancellationTokenSource _stop;
    private readonly Func<bool>? _stallTolerable;
    private readonly TimeSpan _timeout;
    private readonly Task _watch;
    private long _lastVideo = Stopwatch.GetTimestamp();
    private int _expired;

    internal VideoProgressDeadline(CancellationTokenSource preview, Func<bool>? stallTolerable = null,
        TimeSpan? timeout = null)
    {
        _preview = preview;
        _stallTolerable = stallTolerable;
        _timeout = timeout ?? DefaultTimeout;
        if (_timeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeout));
        _stop = CancellationTokenSource.CreateLinkedTokenSource(preview.Token);
        _watch = WatchAsync();
    }

    internal bool Expired => Volatile.Read(ref _expired) != 0;

    internal void Observe(MediaFrame frame)
    {
        if (frame is VideoFrame) Interlocked.Exchange(ref _lastVideo, Stopwatch.GetTimestamp());
    }

    private async Task WatchAsync()
    {
        // At most 4 observations/s per preview in production. Shorter injected
        // policies keep the same bound for a timeout without slowing fixtures.
        var tick = TimeSpan.FromMilliseconds(Math.Min(250, Math.Max(5, _timeout.TotalMilliseconds / 4)));
        using var timer = new PeriodicTimer(tick);
        try
        {
            while (await timer.WaitForNextTickAsync(_stop.Token).ConfigureAwait(false))
            {
                if (_stallTolerable?.Invoke() == true)
                {
                    // Privacy has its own explicit state. When it is lifted,
                    // allow one full first-video interval instead of expiring
                    // immediately on the age of the last pre-privacy image.
                    Interlocked.Exchange(ref _lastVideo, Stopwatch.GetTimestamp());
                    continue;
                }
                if (Stopwatch.GetElapsedTime(Interlocked.Read(ref _lastVideo)) < _timeout) continue;
                Interlocked.Exchange(ref _expired, 1);
                _preview.Cancel();
                return;
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        try { await _watch.ConfigureAwait(false); }
        finally { _stop.Dispose(); }
    }
}
