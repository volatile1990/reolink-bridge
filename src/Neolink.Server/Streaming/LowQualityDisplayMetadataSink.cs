// Reolink Bridge native LQ display metadata; AGPL-3.0.
using Neolink.Media;

namespace Neolink.Streaming;

/// <summary>Changes only native LQ H264 SPS display metadata, retaining all encoded pictures and audio.</summary>
internal sealed class LowQualityDisplayMetadataSink(IMediaSink output, int numerator, int denominator) : IMediaSink
{
    private byte[]? _sourceSps;
    private byte[]? _displaySps;
    private bool _ready;

    public void PublishInfo(MediaInfo info) => output.PublishInfo(info);

    public void PublishVideo(VideoFrame frame)
    {
        if (frame.Codec != VideoCodec.H264)
        {
            Reset();
            return;
        }
        try
        {
            var nals = H26x.SplitNals(frame.Data);
            bool changed = false;
            var rewritten = new List<ReadOnlyMemory<byte>>(nals.Count);
            foreach (var nal in nals)
            {
                if (H26x.H264NalType(nal.Span) != H26x.H264Sps)
                {
                    rewritten.Add(nal);
                    continue;
                }
                if (_sourceSps == null || !nal.Span.SequenceEqual(_sourceSps))
                {
                    var display = H264SampleAspectRatio.RewriteSps(nal.Span, numerator, denominator);
                    _sourceSps = nal.ToArray();
                    _displaySps = display;
                }
                rewritten.Add(_displaySps!);
                changed = true;
                _ready = true;
            }
            if (!_ready) return;
            if (!changed)
            {
                output.PublishVideo(frame);
                return;
            }
            using var data = new MemoryStream();
            foreach (var nal in rewritten)
            {
                data.Write(new byte[] { 0, 0, 0, 1 });
                data.Write(nal.Span);
            }
            output.PublishVideo(frame with { Data = data.ToArray() });
        }
        catch (InvalidDataException)
        {
            // A malformed optional LQ header must never stop Main, alarms or snapshots.
            Reset();
        }
    }

    public void PublishAac(AacFrame frame) => output.PublishAac(frame);
    public void PublishAdpcm(AdpcmFrame frame) => output.PublishAdpcm(frame);
    public void SourceStopped() => Reset();
    public void SourceAuthenticationFailed()
    {
        _sourceSps = _displaySps = null;
        _ready = false;
        output.SourceAuthenticationFailed();
    }
    private void Reset()
    {
        _sourceSps = _displaySps = null;
        _ready = false;
        output.SourceStopped();
    }
}
