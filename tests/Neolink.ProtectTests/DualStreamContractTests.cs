using System.Text.Json.Nodes;
using Neolink.Config;
using Neolink.Media;
using Neolink.Onvif;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class DualStreamContractTests
{
    public static Task Configurations(string example)
    {
        var single = Load(example, _ => { });
        var native = Load(example, input => AddLow(input, "H264", transcode: false));
        var converted = Load(example, input => AddLow(input, "H265", transcode: true));
        foreach (var config in new[] { native, converted })
        {
            var onvif = config.Onvif!;
            Check(config.Cameras.Count == 1 && onvif.Profiles.Count == 2, "dual quality creates another camera");
            Check(onvif.Uuid == single.Onvif!.Uuid && onvif.Mac == single.Onvif.Mac
                && onvif.AdvertisedHost == single.Onvif.AdvertisedHost, "dual quality changes device identity");
            Check(config.Recording == null && config.Mqtt == null && config.WakeHints == null,
                "dual quality starts unrelated services");
        }
        Check(native.Onvif!.LowQualityTranscode == null, "native H264 route unexpectedly converts video");
        Check(converted.Onvif!.LowQualityTranscode == "H265" && converted.Onvif.LowQualitySourceFps == 20,
            "H265 conversion parameters were lost while parsing");
        Check(converted.Onvif.LowQualityEncoder == "software" && converted.Onvif.LowQualityDevice == "/dev/dri/renderD128",
            "existing configurations changed encoder backend");
        var hardware = Load(example, input =>
        {
            AddLow(input, "H265", true);
            input["onvif"]!["low_quality_encoder"] = "vaapi";
            input["onvif"]!["low_quality_device"] = "/dev/dri/renderD129";
            input["onvif"]!["profiles"]![1]!["width"] = 672;
            input["onvif"]!["profiles"]![1]!["height"] = 384;
        });
        Check(hardware.Onvif!.LowQualityEncoder == "vaapi" && hardware.Onvif.LowQualityDevice == "/dev/dri/renderD129"
            && hardware.Onvif.Profiles[1].Width == 672 && hardware.Onvif.Profiles[1].Height == 384,
            "hardware backend/device/output resolution were lost while parsing");
        Load(example, input => { AddLow(input, "H264", false); input["cameras"]![0]!["stream"] = "both"; });
        return Task.CompletedTask;
    }

    public static Task InvalidConfigurations(string example)
    {
        Reject(example, input => { AddLow(input, "H265", true); input["onvif"]!["profiles"]![1]!["codec"] = "H264"; });
        Reject(example, input => input["onvif"]!["low_quality_transcode"] = "H265");
        Reject(example, input => { AddLow(input, "H264", false); input["onvif"]!["low_quality_transcode"] = "H264"; });
        Reject(example, input => { AddLow(input, "H265", true); input["onvif"]!["low_quality_source_fps"] = 0; });
        Reject(example, input => { AddLow(input, "H265", true); input["onvif"]!["low_quality_source_fps"] = 61; });
        Reject(example, input => { AddLow(input, "H265", true); input["onvif"]!["profiles"]![1]!["fps"] = 31; });
        Reject(example, input => { AddLow(input, "H265", true); input["onvif"]!["profiles"]![1]!["bitrate"] = 63; });
        Reject(example, input => { AddLow(input, "H264", false); input["onvif"]!["profiles"]![1]!["stream"] = "externStream"; });
        Reject(example, input => input["cameras"]![0]!["stream"] = "both");
        Reject(example, input => { AddLow(input, "H264", false); input["onvif"]!["profiles"]![0]!["codec"] = "H265"; });
        Reject(example, input => { AddLow(input, "H265", true); input["onvif"]!["low_quality_encoder"] = "auto"; });
        Reject(example, input => { AddLow(input, "H264", false); input["onvif"]!["low_quality_encoder"] = "vaapi"; });
        Reject(example, input => { AddLow(input, "H265", true); input["onvif"]!["low_quality_device"] = "/tmp/renderD128"; });
        Reject(example, input => { AddLow(input, "H265", true); input["onvif"]!["profiles"]![1]!["width"] = 895; });
        return Task.CompletedTask;
    }

    public static Task IndependentStreams(string example)
    {
        using var lifetime = new CancellationTokenSource();
        var singleConfig = Load(example, _ => { });
        var single = ProtectBridgeHost.CreateStreams(singleConfig.Cameras[0], singleConfig.Onvif!, lifetime.Token);
        Check(single.OnvifStreams.Count == 1 && single.MainSource.SecondaryVideoSink == null && single.Transcoder == null,
            "single-profile compatibility started another upstream");
        var nativeConfig = Load(example, input => AddLow(input, "H264", false));
        var native = ProtectBridgeHost.CreateStreams(nativeConfig.Cameras[0], nativeConfig.Onvif!, lifetime.Token);
        Check(native.Transcoder == null && ReferenceEquals(native.LowHub, native.SubSourceHub), "native substream is not forwarded");
        var convertedConfig = Load(example, input => AddLow(input, "H265", true));
        var converted = ProtectBridgeHost.CreateStreams(convertedConfig.Cameras[0], convertedConfig.Onvif!, lifetime.Token);
        Check(converted.MainSource.Kind == Neolink.Protocol.StreamKind.Main
            && converted.MainSource.SecondaryVideoSink != null, "shared-session quality selection is wrong");
        Check(converted.Transcoder != null && !ReferenceEquals(converted.LowHub, converted.SubSourceHub)
            && !ReferenceEquals(converted.LowHub, converted.MainHub), "conversion can replace the main stream");
        Check(converted.OnvifStreams[0].Token == "main" && ReferenceEquals(converted.OnvifStreams[0].Hub, converted.MainHub)
            && converted.OnvifStreams[1].Token == "sub" && ReferenceEquals(converted.OnvifStreams[1].Hub, converted.LowHub),
            "ONVIF profiles point at incorrect media");
        Check(converted.OnvifStreams[0].RequiredForHealth && !converted.OnvifStreams[1].RequiredForHealth,
            "low warmup can take the main stream unhealthy");
        Check(converted.MainSource.RelayOnly && converted.MainSource.MotionSink == null,
            "shared relay owns unrelated controls or duplicates camera events");
        return Task.CompletedTask;
    }

    public static Task MainMounts(string example)
    {
        using var lifetime = new CancellationTokenSource();
        var config = Load(example, input => AddLow(input, "H264", false));
        var pipeline = ProtectBridgeHost.CreateStreams(config.Cameras[0], config.Onvif!, lifetime.Token);
        var server = new RtspServer(new Dictionary<string, string>());
        var allowed = new HashSet<string> { "synthetic" };
        string camera = config.Cameras[0].Name;
        ProtectBridgeHost.AddRtspMounts(server, camera, pipeline.MainHub, allowed, true, 2500, pipeline.LowHub);
        foreach (string path in new[] { "/" + camera, "/" + camera + "/mainStream" })
        {
            var mount = server.FindMount(path);
            Check(mount != null && ReferenceEquals(mount.Hub, pipeline.MainHub) && mount.GopPlayout
                && mount.PlayoutDelayMs == 2500 && ReferenceEquals(mount.PermittedUsers, allowed),
                "main aliases or their established playout policy changed");
        }
        var low = server.FindMount("/" + camera + "/subStream");
        Check(low != null && ReferenceEquals(low.Hub, pipeline.LowHub) && !low.GopPlayout
            && ReferenceEquals(low.PermittedUsers, allowed), "low route lacks its own media, audio or authentication");
        return Task.CompletedTask;
    }

    public static Task AudioAndSourceLifecycle()
    {
        var source = new SinkProbe();
        var output = new SinkProbe();
        var sink = new ProtectBridgeHost.LowQualitySourceSink(source, output);
        var info = new MediaInfo(896, 512, 20);
        var video = new VideoFrame(VideoCodec.H264, true, 10, null, new byte[] { 1, 2, 3 });
        var aac = new AacFrame(new byte[] { 4, 5, 6 });
        var adpcm = new AdpcmFrame(new byte[] { 7, 8, 9 });
        sink.PublishInfo(info); sink.PublishVideo(video); sink.PublishAac(aac); sink.PublishAdpcm(adpcm);
        Check(ReferenceEquals(source.Info, info) && output.Info == null,
            "native input metadata overwrote converted output dimensions/rate");
        Check(ReferenceEquals(source.Video, video) && output.Video == null, "unconverted H264 video leaked to H265 output");
        Check(ReferenceEquals(source.Aac, aac) && ReferenceEquals(output.Aac, aac)
            && ReferenceEquals(source.Adpcm, adpcm) && ReferenceEquals(output.Adpcm, adpcm),
            "original low audio was lost or reencoded");
        sink.SourceStopped(); sink.SourceAuthenticationFailed();
        Check(source.Stops == 1 && output.Stops == 1 && source.AuthFailures == 1 && output.AuthFailures == 1,
            "low source stop or credential failure leaves stale media alive");
        return Task.CompletedTask;
    }

    public static Task OptionalLowHealth(string example)
    {
        var config = Load(example, input => AddLow(input, "H264", false));
        var main = new TestHub("synthetic main");
        var low = new TestHub("synthetic low");
        var streams = new[]
        {
            new ProtectOnvifStream("main", "/synthetic/mainStream", main),
            new ProtectOnvifStream("sub", "/synthetic/subStream", low, RequiredForHealth: false)
        };
        var server = new ProtectOnvifServer(config.Onvif!, new Dictionary<string, string>(), streams, 18554);
        main.Set(VideoCodec.H264, 4512, 2512, true);
        Check(server.Healthy, "low startup hides a healthy main stream");
        low.Set(VideoCodec.H265, 896, 512, true);
        Check(server.Healthy, "optional low codec fault hides a healthy main stream");
        low.Set(VideoCodec.H264, 896, 512, false);
        Check(server.Healthy, "low converter outage hides a healthy main stream");
        main.Set(VideoCodec.H264, 4512, 2512, false);
        low.Set(VideoCodec.H264, 896, 512, true);
        Check(!server.Healthy, "low readiness disguises an unhealthy main stream");
        bool rejected = false;
        try
        {
            _ = new ProtectOnvifServer(config.Onvif!, new Dictionary<string, string>(),
                streams.Select(s => s with { RequiredForHealth = false }).ToArray(), 18554);
        }
        catch (ArgumentException) { rejected = true; }
        Check(rejected, "server with no authoritative health source was accepted");
        return Task.CompletedTask;
    }

    private static void AddLow(JsonObject input, string codec, bool transcode)
    {
        var onvif = input["onvif"]!.AsObject();
        onvif["profiles"]![0]!["codec"] = codec;
        onvif["profiles"]!.AsArray().Add(new JsonObject
        {
            ["stream"] = "subStream", ["codec"] = codec, ["width"] = 896, ["height"] = 512,
            ["fps"] = transcode ? 10 : 20, ["bitrate"] = 512
        });
        if (transcode) { onvif["low_quality_transcode"] = "H265"; onvif["low_quality_source_fps"] = 20; }
    }

    private static NeolinkConfig Load(string example, Action<JsonObject> mutate)
    {
        var input = JsonNode.Parse(File.ReadAllText(example))!.AsObject();
        mutate(input);
        string path = Path.Combine(Path.GetTempPath(), "neolink-dual-test-" + Guid.NewGuid().ToString("N") + ".json");
        File.WriteAllText(path, input.ToJsonString());
        try { return NeolinkConfig.Load(path, strict: true); }
        finally { File.Delete(path); }
    }

    private static void Reject(string example, Action<JsonObject> mutate)
    {
        bool rejected = false;
        try { Load(example, mutate); }
        catch (FormatException) { rejected = true; }
        Check(rejected, "unsafe dual-quality configuration was accepted");
    }

    private static void Check(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }

    private sealed class SinkProbe : IMediaSink
    {
        public VideoFrame? Video;
        public MediaInfo? Info;
        public AacFrame? Aac;
        public AdpcmFrame? Adpcm;
        public int Stops, AuthFailures;
        public void PublishInfo(MediaInfo info) => Info = info;
        public void PublishVideo(VideoFrame frame) => Video = frame;
        public void PublishAac(AacFrame frame) => Aac = frame;
        public void PublishAdpcm(AdpcmFrame frame) => Adpcm = frame;
        public void SourceStopped() => Stops++;
        public void SourceAuthenticationFailed() => AuthFailures++;
    }
}
