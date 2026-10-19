// Source arrival provenance for one-to-one LQ pictures; AGPL-3.0.
namespace Neolink.Streaming;

/// <summary>One-to-one input timing carried around the timestamp-free encoder pipe.</summary>
internal readonly record struct LowQualityPictureTiming(uint Microseconds, long SourceArrivalTimestamp,
    long SourceArrivalTimestampFrequency);

/// <summary>Raw elementary-stream pipes lose timestamps. For a one-to-one encoder,
/// retain the observed source elapsed time outside ffmpeg. Camera clocks
/// are used only for the initial numeric origin, never for duration: some cameras
/// jump their microsecond field independently of sound/video delivery. Bundled
/// pictures share the measured time since the preceding source packet. Individual
/// TCP packets use a bounded phase correction rather than copying network stalls
/// and read bursts into picture durations. The first bundle uses the configured
/// ceiling until a second arrival establishes cadence. No GOP lookahead is added.
/// This is an arrival estimate, not a recovered camera capture timestamp.</summary>
internal sealed class LowQualitySourceTimeline(int frameRate)
{
    private long? _lastArrival;
    private long _firstArrival;
    private long _frequency;
    private long _elapsedUs, _initialBundleUs;
    private uint _origin;
    public long Pictures { get; private set; }

    public uint[] Map(HubVideo video, int pictureCount) => MapWithProvenance(video, pictureCount)
        .Select(value => value.Microseconds).ToArray();

    public LowQualityPictureTiming[] MapWithProvenance(HubVideo video, int pictureCount)
    {
        if (pictureCount is < 1 or > 900 || video.ArrivalTimestamp == null || video.ArrivalTimestampFrequency <= 0)
            throw new InvalidDataException("LQ source lacks bounded picture arrival metadata");
        var output = new LowQualityPictureTiming[pictureCount];
        LowQualityPictureTiming Timing(long elapsedUs) => new(unchecked(_origin + (uint)elapsedUs),
            checked(_firstArrival + (long)Math.Round((elapsedUs - _initialBundleUs)
                * (double)_frequency / 1_000_000)), _frequency);
        if (_lastArrival == null)
        {
            _origin = video.CameraMicroseconds ?? (uint)((ulong)video.RtpTs * 1000 / 90);
            _frequency = video.ArrivalTimestampFrequency;
            _firstArrival = video.ArrivalTimestamp.Value;
            _elapsedUs = _initialBundleUs = (pictureCount - 1) * 1_000_000L / frameRate;
            for (int i = 0; i < pictureCount; i++)
                output[i] = Timing((long)i * 1_000_000 / frameRate);
        }
        else
        {
            long ticks = video.ArrivalTimestamp.Value - _lastArrival.Value;
            if (video.ArrivalTimestampFrequency != _frequency || ticks < 0 || ticks > _frequency * 5)
                throw new InvalidDataException("LQ source arrival clock changed or disconnected");
            // Protect reduces the 90 kHz RTP clock to millisecond timestamps. The
            // former 12-us minimum survived RTP but became a zero-duration sample
            // after a TCP read burst. Equally, copying a 767-ms network gap into
            // one picture made the player hold that picture for 767 ms. Correct
            // bounded phase error against an absolute arrival anchor, so minimum
            // advances are repaid instead of accumulating clock drift.
            long nominalUs = 1_000_000L / frameRate;
            long targetUs = checked(_initialBundleUs + (long)Math.Round(
                (video.ArrivalTimestamp.Value - _firstArrival) * 1_000_000d / _frequency));
            long nominalSpan = checked(nominalUs * pictureCount);
            double correction = (targetUs - (_elapsedUs + nominalSpan)) / (pictureCount == 1 ? 4d : 1d);
            long minimumSpan = checked(Math.Max(2_000, nominalUs / 4) * pictureCount);
            // Keep the nominal duration inside the interval, including valid 1–4 fps sources.
            long maximumSpan = checked(Math.Max(nominalUs, Math.Min(200_000, nominalUs * 4)) * pictureCount);
            long spanUs = (long)Math.Clamp(Math.Round(nominalSpan + correction), minimumSpan, maximumSpan);
            for (int i = 0; i < pictureCount; i++)
                output[i] = Timing(_elapsedUs + spanUs * (i + 1) / pictureCount);
            _elapsedUs += spanUs;
        }
        _lastArrival = video.ArrivalTimestamp;
        Pictures += pictureCount;
        return output;
    }
}
