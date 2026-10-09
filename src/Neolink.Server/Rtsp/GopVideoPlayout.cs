// Reolink Bridge: bounded, opt-in complete-GOP video playout; AGPL-3.0, see LICENSE.
using Neolink.Media;
using Neolink.Streaming;

namespace Neolink.Rtsp;

internal sealed record GopPlayoutPicture(byte[] AnnexB, bool Keyframe, bool HasSps,
    uint RtpTimestamp, TimeSpan Offset, long SourceEpoch);
internal sealed record CompletedVideoGop(IReadOnlyList<GopPlayoutPicture> Pictures, TimeSpan Duration, long SourceEpoch);

/// <summary>One RTSP pump owns this object. It releases only a complete bounded GOP,
/// scales its camera PTS onto the measured keyframe-arrival wall duration, and paces
/// the new outbound timeline. Source timestamps and encoded NAL contents stay intact.
/// One complete output GOP plus the next keyframe are at most two 6-MiB groups;
/// the existing bounded subscriber channel remains the source/backpressure boundary.</summary>
internal sealed class GopVideoPlayout
{
    private const int MaxBytes = 6 * 1024 * 1024, MaxFrames = 900;
    private const uint MaxRtpSpan = 5 * 90000;
    private readonly VideoCodec _codec;
    private readonly TimeSpan _reserve;
    private readonly TimeProvider _clock;
    private readonly Func<TimeSpan, CancellationToken, Task> _wait;
    private readonly List<SourceBuffer> _buffers = new();
    private readonly List<RtpAccessUnit> _prefixMetadata = new();
    private int _prefixBytes;
    private long? _prefixEpoch;
    private int _bytes, _pictures;
    private bool _haveOutputTimestamp, _scheduleStarted;
    private uint _nextOutputTimestamp;
    private long _scheduleEpoch;
    private TimeSpan _scheduledDuration, _currentGopOffset;
    private bool _haveLastSent;
    private uint _lastSentTimestamp;
    private long _lastSentAt;
    public int BufferedBytes => _bytes + _prefixBytes;
    public int BufferedFrames => _pictures;

    private sealed record SourceBuffer(HubVideo Video, List<RtpAccessUnit> Units, int Pictures);

    public GopVideoPlayout(VideoCodec codec, int reserveMs, TimeProvider? clock = null,
        Func<TimeSpan, CancellationToken, Task>? wait = null)
    {
        if (reserveMs is < 0 or > 5000) throw new ArgumentOutOfRangeException(nameof(reserveMs));
        _codec = codec; _reserve = TimeSpan.FromMilliseconds(reserveMs);
        _clock = clock ?? TimeProvider.System;
        _wait = wait ?? ((delay, ct) => Task.Delay(delay, _clock, ct));
    }

    /// <summary>Discard pending input and scheduling state; keep the outbound RTP clock continuous.</summary>
    public void Reset()
    {
        _buffers.Clear(); _bytes = _pictures = 0;
        _prefixMetadata.Clear(); _prefixBytes = 0; _prefixEpoch = null;
        _scheduleStarted = false; _scheduledDuration = _currentGopOffset = TimeSpan.Zero;
    }

    public CompletedVideoGop? Push(HubVideo video)
    {
        if (video.ArrivalTimestamp == null || video.ArrivalTimestampFrequency <= 0 || video.AnnexB.Length > MaxBytes)
        { Reset(); return null; }
        var units = RtpAccessUnitSplitter.Split(_codec, video.AnnexB);
        var incoming = new SourceBuffer(video, units, units.Count(unit => unit.HasVcl));
        if (incoming.Pictures > MaxFrames) { Reset(); return null; }
        if (_buffers.Count == 0)
        {
            if (incoming.Pictures == 0)
            {
                if (_prefixEpoch != video.SourceEpoch) { _prefixMetadata.Clear(); _prefixBytes = 0; }
                if (_prefixBytes + video.AnnexB.Length > MaxBytes) { Reset(); return null; }
                _prefixEpoch = video.SourceEpoch; _prefixMetadata.AddRange(units); _prefixBytes += video.AnnexB.Length;
            }
            else if (video.Keyframe)
            {
                if (_prefixEpoch == video.SourceEpoch && _prefixBytes + video.AnnexB.Length <= MaxBytes)
                {
                    incoming.Units.InsertRange(0, _prefixMetadata); _bytes += _prefixBytes;
                }
                _prefixMetadata.Clear(); _prefixBytes = 0; _prefixEpoch = null;
                Append(incoming);
            }
            return null;
        }
        var first = _buffers[0].Video;
        var last = _buffers[^1].Video;
        uint step = unchecked(video.RtpTs - last.RtpTs);
        uint span = unchecked(video.RtpTs - first.RtpTs);
        // Metadata may legitimately have the same clock as the preceding picture.
        // It is carried forward without making a picture or advancing the timing anchor.
        if (incoming.Pictures == 0)
        {
            if (video.SourceEpoch != first.SourceEpoch || video.ArrivalTimestampFrequency != first.ArrivalTimestampFrequency
                || video.ArrivalTimestamp < last.ArrivalTimestamp || ArrivalSeconds(first, video) is < 0 or > 5
                || _bytes + video.AnnexB.Length > MaxBytes)
            { Reset(); return null; }
            last = _buffers[^1].Video;
            _buffers[^1].Units.AddRange(units); _bytes += video.AnnexB.Length;
            return null;
        }
        bool cameraSane = video.CameraMicroseconds == null || last.CameraMicroseconds == null || first.CameraMicroseconds == null
            || (unchecked(video.CameraMicroseconds.Value - last.CameraMicroseconds.Value) is > 0 and <= 5_000_000
                && unchecked(video.CameraMicroseconds.Value - first.CameraMicroseconds.Value) is > 0 and <= 5_000_000);
        bool sane = video.SourceEpoch == first.SourceEpoch
            && video.ArrivalTimestampFrequency == first.ArrivalTimestampFrequency
            && video.ArrivalTimestamp >= last.ArrivalTimestamp
            && ArrivalSeconds(first, video) is >= 0 and <= 5
            && step is > 0 and <= MaxRtpSpan && span is > 0 and <= MaxRtpSpan && cameraSane;
        if (!sane)
        {
            Reset();
            if (video.Keyframe && incoming.Pictures > 0) Append(incoming);
            return null;
        }
        if (video.Keyframe)
        {
            var completed = Complete(video, span);
            _buffers.Clear(); _bytes = _pictures = 0;
            if (completed == null) Reset();
            if (incoming.Pictures > 0) Append(incoming);
            return completed;
        }
        if (_bytes + video.AnnexB.Length > MaxBytes || _buffers.Count == MaxFrames || _pictures + incoming.Pictures > MaxFrames)
        { Reset(); return null; }
        Append(incoming);
        return null;
    }

    private void Append(SourceBuffer buffer)
    {
        _buffers.Add(buffer); _bytes += buffer.Video.AnnexB.Length; _pictures += buffer.Pictures;
    }

    private static double ArrivalSeconds(HubVideo first, HubVideo last) =>
        unchecked(last.ArrivalTimestamp!.Value - first.ArrivalTimestamp!.Value) / (double)first.ArrivalTimestampFrequency;

    private CompletedVideoGop? Complete(HubVideo closingKey, uint sourceSpan)
    {
        var first = _buffers[0].Video;
        double seconds = ArrivalSeconds(first, closingKey);
        if (seconds <= 0 || _pictures == 0) return null;
        long durationTicks = (long)Math.Round(seconds * 90000);
        if (durationTicks < _pictures || durationTicks > MaxRtpSpan) return null;
        uint start = _haveOutputTimestamp ? _nextOutputTimestamp : first.RtpTs;
        var pending = new List<ReadOnlyMemory<byte>>();
        var pictures = new List<GopPlayoutPicture>(_pictures);
        int outputBytes = 0;
        long previousPosition = -1;
        for (int bufferIndex = 0; bufferIndex < _buffers.Count; bufferIndex++)
        {
            var buffer = _buffers[bufferIndex];
            uint offset = unchecked(buffer.Video.RtpTs - first.RtpTs);
            uint nextOffset = bufferIndex + 1 < _buffers.Count
                ? unchecked(_buffers[bufferIndex + 1].Video.RtpTs - first.RtpTs) : sourceSpan;
            int pictureIndex = 0;
            foreach (var unit in buffer.Units)
            {
                pending.AddRange(unit.Nals);
                if (!unit.HasVcl) continue;
                // A bundled buffer has one source timestamp. Spread its pictures
                // over the known interval up to the next buffer, never a guessed fixed FPS.
                double sourcePosition = offset + (nextOffset - offset) * (pictureIndex / (double)buffer.Pictures);
                long position = (long)Math.Round(sourcePosition * durationTicks / sourceSpan);
                if (position <= previousPosition || position >= durationTicks) return null;
                previousPosition = position;
                int bytes = pending.Sum(nal => checked(4 + nal.Length));
                if (outputBytes + bytes > MaxBytes) return null;
                outputBytes += bytes;
                bool key = unit.Keyframe || (buffer.Video.Keyframe && pictureIndex == 0);
                bool hasSps = pending.Any(nal => nal.Length > 0 &&
                    (_codec == VideoCodec.H265 ? H26x.H265NalType(nal.Span) == 33 : H26x.H264NalType(nal.Span) == 7));
                pictures.Add(new GopPlayoutPicture(RtpAccessUnitSplitter.AnnexB(pending), key, hasSps,
                    unchecked(start + (uint)position), TimeSpan.FromSeconds(position / 90000d), first.SourceEpoch));
                pending.Clear(); pictureIndex++;
            }
        }
        // Metadata at the very end is still transported, without inventing a picture.
        if (pending.Count > 0)
        {
            int tailBytes = pending.Sum(nal => checked(4 + nal.Length));
            if (outputBytes + tailBytes > MaxBytes) return null;
            var old = pictures[^1];
            byte[] tail = RtpAccessUnitSplitter.AnnexB(pending);
            byte[] joined = new byte[old.AnnexB.Length + tail.Length];
            old.AnnexB.CopyTo(joined, 0); tail.CopyTo(joined, old.AnnexB.Length);
            pictures[^1] = old with { AnnexB = joined };
        }
        _haveOutputTimestamp = true;
        _nextOutputTimestamp = unchecked(start + (uint)durationTicks);
        return new CompletedVideoGop(pictures, TimeSpan.FromSeconds(durationTicks / 90000d), first.SourceEpoch);
    }

    public CompletedVideoGop BeginGop(CompletedVideoGop gop)
    {
        var elapsed = _scheduleStarted ? _clock.GetElapsedTime(_scheduleEpoch, _clock.GetTimestamp()) : TimeSpan.Zero;
        if (!_scheduleStarted || elapsed - (_reserve + _scheduledDuration) > TimeSpan.FromSeconds(5))
        {
            _scheduleEpoch = _clock.GetTimestamp(); _scheduledDuration = TimeSpan.Zero; _scheduleStarted = true;
            // Recover from the ACTUAL last transmitted picture, not an end timestamp
            // reserved for a GOP whose remaining pictures may have been cancelled.
            if (_haveLastSent)
            {
                double seconds = Math.Max(0, _clock.GetElapsedTime(_lastSentAt, _scheduleEpoch).TotalSeconds + _reserve.TotalSeconds);
                long recoveryTicks = Math.Max(1, (long)Math.Round(seconds * 90000));
                uint next = unchecked(_lastSentTimestamp + (uint)recoveryTicks);
                uint adjustment = unchecked(next - gop.Pictures[0].RtpTimestamp);
                gop = gop with { Pictures = gop.Pictures.Select(picture => picture with
                    { RtpTimestamp = unchecked(picture.RtpTimestamp + adjustment) }).ToArray() };
                _nextOutputTimestamp = unchecked(_nextOutputTimestamp + adjustment);
            }
        }
        _currentGopOffset = _scheduledDuration;
        _scheduledDuration += gop.Duration;
        return gop;
    }

    public void MarkSent(GopPlayoutPicture picture)
    {
        _haveLastSent = true; _lastSentTimestamp = picture.RtpTimestamp; _lastSentAt = _clock.GetTimestamp();
    }

    public async Task WaitAsync(TimeSpan offset, CancellationToken ct)
    {
        if (!_scheduleStarted) throw new InvalidOperationException("BeginGop must precede output");
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var remaining = _reserve + _currentGopOffset + offset
                - _clock.GetElapsedTime(_scheduleEpoch, _clock.GetTimestamp());
            if (remaining <= TimeSpan.Zero) return;
            await _wait(remaining, ct).ConfigureAwait(false);
        }
    }
}
