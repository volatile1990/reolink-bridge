using System.Text.Json;
using Neolink.Config;
using Neolink.Media;
using Neolink.Streaming;

internal static class LowQualityDisplayMetadataTests
{
    public static Task IsolationAndRecovery()
    {
        var output = new RecordingSink();
        var sink = new LowQualityDisplayMetadataSink(output, 4, 3);
        var info = new MediaInfo(640, 480, 15);
        var aac = new AacFrame([1, 2, 3]);
        var adpcm = new AdpcmFrame([4, 5, 6]);
        sink.PublishInfo(info); sink.PublishAac(aac); sink.PublishAdpcm(adpcm);
        Check(ReferenceEquals(output.Info, info) && ReferenceEquals(output.Aac, aac)
            && ReferenceEquals(output.Adpcm, adpcm), "metadata transform changed dimensions or audio");

        var predicted = new VideoFrame(VideoCodec.H264, false, 123456, 987654, [0, 0, 0, 1, 0x41, 0x80]);
        sink.PublishVideo(predicted);
        Check(output.Video.Count == 0, "unmarked LQ pictures escaped before display SPS");
        byte[] sourceSps = H264SampleAspectRatioTests.SyntheticSps();
        byte[] picture = [0x65, 0x88, 0x80];
        var original = new VideoFrame(VideoCodec.H264, true, 123457, 987655, AnnexB(sourceSps, picture));
        byte[] before = original.Data.ToArray();
        sink.PublishVideo(original);
        Check(original.Data.SequenceEqual(before), "optional LQ transform mutated original shared media");
        var marked = output.Video.Single();
        Check(marked.Codec == original.Codec && marked.Keyframe == original.Keyframe
            && marked.Microseconds == original.Microseconds && marked.UnixTime == original.UnixTime,
            "display transform changed frame timing or identity");
        var nals = H26x.SplitNals(marked.Data);
        Check(nals.Count == 2 && nals[1].Span.SequenceEqual(picture), "display transform changed coded picture");
        Check(!nals[0].Span.SequenceEqual(sourceSps), "SPS display metadata did not change");
        Check(H26x.TryGetDimensions(VideoCodec.H264, nals[0].Span, out uint width, out uint height)
            && width == 640 && height == 480, "display transform changed encoded resolution");
        sink.PublishVideo(predicted);
        Check(ReferenceEquals(output.Video.Last(), predicted), "predicted picture was copied or changed");

        sink.PublishVideo(original with { Data = [0, 0, 0, 1, 0x67, 0x00] });
        Check(output.Stops == 1, "malformed optional SPS did not isolate LQ failure");
        int count = output.Video.Count;
        sink.PublishVideo(predicted);
        Check(output.Video.Count == count, "unmarked video escaped after failed metadata update");
        sink.PublishVideo(original);
        Check(output.Video.Count == count + 1, "valid SPS did not recover LQ");
        sink.SourceStopped(); sink.SourceAuthenticationFailed();
        Check(output.Stops == 2 && output.AuthenticationFailures == 1, "source lifecycle was not forwarded");
        return Task.CompletedTask;
    }

    public static Task ConfigurationGuards()
    {
        BridgeOnvifConfig Make() => new()
        {
            AdvertisedHost = "127.0.0.1", Mac = "02:42:00:00:00:01",
            Uuid = "a4e258bb-7917-4915-965a-416016998070", LowQualitySampleAspectRatio = "4:3",
            Profiles = [new() { Stream = "mainStream", Codec = "H264", Width = 2560, Height = 1920, Fps = 20, Bitrate = 5120 },
                new() { Stream = "subStream", Codec = "H264", Width = 640, Height = 480, Fps = 15, Bitrate = 512 }]
        };
        var valid = Make(); valid.Validate();
        using var document = JsonDocument.Parse(JsonSerializer.Serialize(new
        {
            advertised_host = valid.AdvertisedHost, mac = valid.Mac, uuid = valid.Uuid,
            low_quality_sample_aspect_ratio = "4:3", profiles = valid.Profiles
        }));
        Check(BridgeOnvifConfig.Parse(document.RootElement).LowQualitySampleAspectRatio == "4:3", "SAR setting lost while parsing");
        void Reject(Action<BridgeOnvifConfig> mutate)
        {
            var config = Make(); mutate(config);
            try { config.Validate(); }
            catch (FormatException) { return; }
            throw new InvalidOperationException("unsafe display metadata configuration accepted");
        }
        Reject(c => c.LowQualitySampleAspectRatio = "16:9");
        Reject(c => c.Profiles.RemoveAt(1));
        Reject(c => c.Profiles[1].Width = 896);
        Reject(c => { c.Profiles.ForEach(p => p.Codec = "H265"); c.LowQualityTranscode = "H265"; });
        Reject(c => c.LowQualityTranscode = "H265");
        return Task.CompletedTask;
    }

    private static byte[] AnnexB(params byte[][] nals)
    {
        using var stream = new MemoryStream();
        foreach (var nal in nals) { stream.Write(new byte[] { 0, 0, 0, 1 }); stream.Write(nal); }
        return stream.ToArray();
    }
    private static void Check(bool value, string message)
    {
        if (!value) throw new InvalidOperationException(message);
    }
    private sealed class RecordingSink : IMediaSink
    {
        public MediaInfo? Info;
        public AacFrame? Aac;
        public AdpcmFrame? Adpcm;
        public List<VideoFrame> Video = [];
        public int Stops, AuthenticationFailures;
        public void PublishInfo(MediaInfo info) => Info = info;
        public void PublishVideo(VideoFrame frame) => Video.Add(frame);
        public void PublishAac(AacFrame frame) => Aac = frame;
        public void PublishAdpcm(AdpcmFrame frame) => Adpcm = frame;
        public void SourceStopped() => Stops++;
        public void SourceAuthenticationFailed() => AuthenticationFailures++;
    }
}
