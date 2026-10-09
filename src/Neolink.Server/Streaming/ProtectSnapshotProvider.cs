// Reolink Bridge: native JPEG snapshots over the existing session; AGPL-3.0.
using System.Buffers.Binary;
using Neolink.Protocol;

namespace Neolink.Streaming;

public interface IProtectSnapshotProvider
{
    Task<byte[]?> GetJpegAsync(CancellationToken ct);
}

/// <summary>One camera's native snapshot cache. Requests share one capture, never
/// connect/login/start a stream, and never cancel another caller's work.</summary>
public sealed class NativeProtectSnapshotProvider : IProtectSnapshotProvider
{
    public const int MaxJpegBytes = 4 * 1024 * 1024;
    private static readonly TimeSpan CacheAge = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan FailureCooldown = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan CaptureBudget = TimeSpan.FromSeconds(3);
    // Process-wide only: separate Docker instances do not share this semaphore.
    private static readonly SemaphoreSlim Captures = new(2, 2);
    private readonly Func<IBcCamera?> _currentCamera;
    private readonly Func<bool> _ready;
    private readonly Func<long> _sourceEpoch;
    private readonly TimeProvider _clock;
    private readonly CancellationToken _lifetime;
    private readonly object _gate = new();
    private IBcCamera? _identity;
    private long _identityEpoch;
    private byte[]? _cached;
    private long _capturedAt;
    private long? _failedAt;
    private Task<byte[]?>? _inFlight;

    public NativeProtectSnapshotProvider(Func<IBcCamera?> currentCamera, Func<bool> ready,
        Func<long>? sourceEpoch = null, TimeProvider? clock = null, CancellationToken lifetime = default)
    {
        _currentCamera = currentCamera; _ready = ready;
        _sourceEpoch = sourceEpoch ?? (() => 0);
        _clock = clock ?? TimeProvider.System; _lifetime = lifetime;
    }

    public async Task<byte[]?> GetJpegAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        Task<byte[]?> pending;
        lock (_gate)
        {
            var camera = _currentCamera();
            long epoch = _sourceEpoch();
            if (!ReferenceEquals(camera, _identity) || epoch != _identityEpoch)
            {
                _identity = camera; _identityEpoch = epoch;
                _cached = null; _failedAt = null;
            }
            if (_lifetime.IsCancellationRequested || camera == null || !_ready())
            { _cached = null; return null; }
            long now = _clock.GetTimestamp();
            if (_cached != null && _clock.GetElapsedTime(_capturedAt, now) < CacheAge)
                return _cached;
            if (_failedAt is { } failed && _clock.GetElapsedTime(failed, now) < FailureCooldown)
                return null;
            if (_inFlight == null || _inFlight.IsCompleted)
                _inFlight = Task.Run(() => CaptureAsync(camera, epoch), CancellationToken.None);
            pending = _inFlight;
        }
        return await pending.WaitAsync(ct).ConfigureAwait(false);
    }

    private async Task<byte[]?> CaptureAsync(IBcCamera camera, long epoch)
    {
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(_lifetime);
        budget.CancelAfter(CaptureBudget);
        byte[]? jpeg = null;
        bool acquired = false;
        try
        {
            await Captures.WaitAsync(budget.Token).ConfigureAwait(false);
            acquired = true;
            if (_ready() && ReferenceEquals(_currentCamera(), camera) && _sourceEpoch() == epoch)
                jpeg = await camera.SnapBoundedAsync(MaxJpegBytes, budget.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is OperationCanceledException or TimeoutException or IOException
            or InvalidOperationException or Neolink.Bc.BcProtocolException or CameraCommandException) { }
        finally { if (acquired) Captures.Release(); }
        lock (_gate)
        {
            bool current = !_lifetime.IsCancellationRequested && _ready()
                && ReferenceEquals(_currentCamera(), camera) && _sourceEpoch() == epoch;
            if (!current) return null;
            if (IsValidJpeg(jpeg))
            {
                _cached = jpeg; _capturedAt = _clock.GetTimestamp(); _failedAt = null;
                return jpeg;
            }
            _cached = null; _failedAt = _clock.GetTimestamp();
            return null;
        }
    }

    /// <summary>Bounded structural validation, not a JPEG decoder: SOI, SOF size,
    /// a scan and terminal EOI. Rejects truncated/error bodies before caching.</summary>
    internal static bool IsValidJpeg(byte[]? bytes)
    {
        if (bytes is not { Length: >= 32 and <= MaxJpegBytes } || bytes[0] != 0xFF || bytes[1] != 0xD8
            || bytes[^2] != 0xFF || bytes[^1] != 0xD9) return false;
        bool haveSof = false;
        int sofComponents = 0;
        int at = 2;
        while (at < bytes.Length - 2)
        {
            if (bytes[at++] != 0xFF) return false;
            while (at < bytes.Length && bytes[at] == 0xFF) at++;
            if (at >= bytes.Length) return false;
            byte marker = bytes[at++];
            if (marker is 0 or 0xD8 or 0xD9 || marker is >= 0xD0 and <= 0xD7) return false;
            if (marker == 0x01) continue;
            if (at + 2 > bytes.Length - 2) return false;
            int length = BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(at, 2));
            if (length < 2 || at + length > bytes.Length - 2) return false;
            bool sof = marker is >= 0xC0 and <= 0xCF && marker is not (0xC4 or 0xC8 or 0xCC);
            if (sof)
            {
                if (length < 8) return false;
                int height = BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(at + 3, 2));
                int width = BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(at + 5, 2));
                int components = bytes[at + 7];
                if (height is < 1 or > 16384 || width is < 1 or > 16384
                    || components is < 1 or > 4 || length < 8 + 3 * components) return false;
                haveSof = true;
                sofComponents = components;
            }
            if (marker == 0xDA)
            {
                if (!haveSof || length < 6) return false;
                int components = bytes[at + 2];
                return components is >= 1 and <= 4 && components <= sofComponents
                    && length == 6 + 2 * components && at + length < bytes.Length - 2;
            }
            at += length;
        }
        return false;
    }
}
