// Copyright (c) 2026 Oluwabori Olaleye
// Licensed under the GNU Affero General Public License v3.0; see the LICENSE file
// in the repository root.
using System.Threading.Channels;
using Neolink.Media;
using Neolink.Streaming;

namespace Neolink.Rtsp;

/// <summary>
/// Receives the RTSP audio backchannel (ONVIF Profile-T two-way talk): G.711 RTP
/// packets the client pushes during PLAY are depacketized, decoded to 16-bit PCM
/// and streamed into the camera's <see cref="ICameraControl.TalkAsync"/> pipeline,
/// which resamples and ADPCM-encodes them to the speaker. One per RTSP session.
/// </summary>
internal sealed class BackchannelReceiver
{
    // G.711 is always 8 kHz; TalkAsync resamples to the camera's talk rate.
    private const int SampleRate = 8000;

    // Talk is real-time: if the camera side stalls, dropping the oldest audio is
    // the right backpressure — never let the queue grow without bound.
    private readonly object _gate = new();
    private readonly ICameraControl _control;
    private Channel<byte[]>? _pcm;
    private CancellationTokenSource? _cts;
    private Task? _talk;
    private long _generation, _retryAt;
    private int _failures;
    private bool _unsupported;
    private readonly TimeProvider _clock;
    private readonly TimeSpan _retryDelay;

    public BackchannelReceiver(ICameraControl control, TimeSpan? retryDelay = null, TimeProvider? clock = null)
    {
        _control = control;
        _clock = clock ?? TimeProvider.System;
        _retryDelay = retryDelay ?? TimeSpan.FromMilliseconds(500);
    }

    private static Channel<byte[]> NewQueue() => Channel.CreateBounded<byte[]>(new BoundedChannelOptions(32)
    { SingleReader = true, SingleWriter = false, FullMode = BoundedChannelFullMode.DropOldest });

    /// <summary>Arms the receive track. The camera talk session starts only on a
    /// valid RTP audio packet, so PLAY alone never sends a speaker command.</summary>
    public void Start(CancellationToken ct)
    {
        lock (_gate)
        {
            if (_cts != null || ct.IsCancellationRequested) return;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _generation++;
            _retryAt = 0; _failures = 0; _unsupported = false;
            _pcm = NewQueue();
        }
    }

    /// <summary>Closes the talk session and releases the camera's talk channel.</summary>
    public void Stop() => _ = StopAsync();

    internal Task StopAsync()
    {
        CancellationTokenSource? stopping;
        Task? talk;
        lock (_gate)
        {
            stopping = _cts;
            talk = _talk;
            _pcm?.Writer.TryComplete();
            _cts = null;
            _pcm = null;
            _talk = null;
            _generation++;
        }
        if (stopping == null) return Task.CompletedTask;
        stopping.Cancel();
        // Let the camera finish its cancellation path before releasing the CTS.
        // Never block the RTSP control reader while the speaker closes.
        return FinishStopAsync(talk, stopping);
    }

    private static async Task FinishStopAsync(Task? talk, CancellationTokenSource stopping)
    {
        try { if (talk != null) await talk.ConfigureAwait(false); }
        catch (OperationCanceledException) { }
        finally { stopping.Dispose(); }
    }

    /// <summary>Feeds one interleaved RTP packet (12-byte header + G.711 payload).</summary>
    public void OnRtp(byte[] packet)
    {
        var pcm = Depacketize(packet);
        if (pcm == null) return;
        OnPcm(pcm);
    }

    /// <summary>Feeds decoded 8 kHz mono PCM from a native AAC talk decoder.</summary>
    internal void OnPcm(byte[] pcm)
    {
        if (pcm.Length is 0 or > 16384 || (pcm.Length & 1) != 0) return;
        lock (_gate)
        {
            // Packets before PLAY or after TEARDOWN must never be retained and
            // played by a later session. Each arming owns a fresh bounded queue.
            if (_cts == null || _cts.IsCancellationRequested || _pcm == null) return;
            if (_unsupported || _clock.GetTimestamp() < _retryAt) return;
            _pcm.Writer.TryWrite(pcm);
            if (_talk != null) return;
            var token = _cts.Token;
            var reader = _pcm.Reader;
            var queue = _pcm;
            long generation = _generation;
            _talk = Task.Run(async () =>
            {
                bool unsupported = false;
                try
                {
                    await _control.TalkAsync(SampleRate, reader, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { }
                catch (TalkBusyException) { Log.Debug($"{_control.CameraName}: backchannel busy (another talk session is active)"); }
                catch (NotSupportedException) { unsupported = true; Log.Debug($"{_control.CameraName}: backchannel unsupported (no speaker)"); }
                catch (Exception ex) { Log.Debug($"{_control.CameraName}: backchannel talk ended: {Log.Flatten(ex)}"); }
                finally
                {
                    lock (_gate)
                    {
                        if (generation == _generation && _cts != null && !token.IsCancellationRequested)
                        {
                            // A failed attempt never retains speech for its successor. Retry requires
                            // a newly received PCM block after the bounded cooldown.
                            queue.Writer.TryComplete();
                            _pcm = NewQueue();
                            _talk = null;
                            _unsupported = unsupported;
                            _failures = Math.Min(4, _failures + 1);
                            double seconds = Math.Min(5, _retryDelay.TotalSeconds * (1 << (_failures - 1)));
                            _retryAt = _clock.GetTimestamp() + (long)(seconds * _clock.TimestampFrequency);
                        }
                    }
                }
            }, CancellationToken.None);
            Log.Info($"{_control.CameraName}: RTSP audio backchannel opened");
        }
    }

    /// <summary>Strips the RTP header and G.711-decodes the payload to 16-bit LE PCM.</summary>
    internal static byte[]? Depacketize(byte[] p)
    {
        if (p.Length < 12 || (p[0] >> 6) != 2) return null; // need a full RTP v2 header
        int cc = p[0] & 0x0F;
        bool ext = (p[0] & 0x10) != 0;
        bool pad = (p[0] & 0x20) != 0;
        int payloadType = p[1] & 0x7F;
        // Only the static G.711 payload types are implemented. Treating AAC,
        // Opus or RTCP as mu-law would turn unrelated bytes into speaker noise.
        if (payloadType is not (0 or 8)) return null;

        int off = 12 + cc * 4;
        if (off > p.Length) return null;
        if (ext)
        {
            if (off + 4 > p.Length) return null;
            int words = (p[off + 2] << 8) | p[off + 3];
            off += 4 + words * 4;
            if (off > p.Length) return null;
        }
        int end = p.Length;
        if (pad)
        {
            if (end <= off) return null;
            int padding = p[end - 1];
            if (padding == 0 || padding > end - off) return null;
            end -= padding;
        }
        if (end <= off) return null;

        // PT 8 = A-law (PCMA); PT 0 = mu-law (PCMU).
        return G711.ToPcm16(p.AsSpan(off, end - off), aLaw: payloadType == 8);
    }
}
