using Neolink.Media;
using Neolink.Rtsp;
using Neolink.Streaming;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

internal static class LowQualityCadenceTests
{
    private static void Check(bool condition, string message) { if (!condition) throw new Exception(message); }
    private static HubVideo Source(uint cameraUs, long arrival, int index = 0) => new(index, [0, 0, 0, 1, 0x65, 0x80], true, 0, false)
    { SourceEpoch = 1, CameraMicroseconds = cameraUs, ArrivalTimestamp = arrival, ArrivalTimestampFrequency = 1_000_000 };

    public static Task SourceCadenceAndBurstClock()
    {
        var night = new LowQualitySourceTimeline(20);
        uint origin = uint.MaxValue - 60_000;
        var times = Enumerable.Range(0, 80).Select(i => night.Map(Source(unchecked(origin + (uint)(i * 50_000)), i * 80_000L), 1)[0]).ToArray();
        Check(times.TakeLast(10).Zip(times.TakeLast(9)).All(pair => Math.Abs((long)unchecked(pair.Second - pair.First) - 80_000) <= 2),
            "12.5 real fps with a configured 20 fps ceiling did not settle to 80 ms, or camera-clock wrap changed duration");
        Check(unchecked(times[^1] - times[0]) is > 6_220_000 and <= 6_320_000 && night.Pictures == 80,
            "night conversion retained the fixed 20-fps clock or discarded pictures");
        Check(Math.Abs((long)unchecked(times[^1] - times[0]) + 80_000 - 100 * 1024 * 1_000_000L / 16000) < 100_000,
            "bounded night phase exceeded the elapsed-time relation to untouched 16kHz AAC units");
        var day = Enumerable.Range(1, 20).Select(i => night.Map(Source(0, 6_320_000 + i * 50_000L), 1)[0]).ToArray();
        Check(Math.Abs((long)unchecked(day[^1] - day[^2]) - 50_000) < 100
            && Math.Abs((long)unchecked(day[^1] - times[0]) - 7_320_000) < 1000,
            "return to 20fps did not repay the bounded night phase error");

        var burst = new LowQualitySourceTimeline(20);
        var a = burst.Map(Source(0, 0), 16);
        var b = burst.Map(Source(8_670_000, 1_280_000), 16);
        var c = burst.Map(Source(17_340_000, 2_560_000), 16);
        Check(a.Length == 16 && b.Length == 16 && c.Length == 16 && burst.Pictures == 48,
            "bundled source pictures vanished");
        Check(unchecked(b[0] - a[^1]) == 80_000 && b.Zip(b.Skip(1)).All(pair => unchecked(pair.Second - pair.First) == 80_000)
            && unchecked(c[^1] - b[^1]) == 1_280_000, "8.67-second camera field jumps leaked into the bundled LQ timeline");
        var duplicateArrival = burst.Map(Source(17_390_000, 2_560_000), 1);
        Check(unchecked(duplicateArrival[0] - c[^1]) >= 12_500, "same-time source packet became a zero Protect millisecond sample");

        // A real TCP socket can deliver forty distinct AUs with the same observed
        // arrival after a stall. The old per-arrival mapping produced one long
        // picture followed by 1-tick RTP samples that Protect rounds to zero ms.
        var arrivals = Enumerable.Range(0, 20).Select(i => i * 50_000L)
            .Concat(Enumerable.Repeat(2_950_000L, 40))
            .Concat(Enumerable.Range(0, 80).Select(i => 3_000_000L + i * 50_000L)).ToArray();
        var transport = new LowQualitySourceTimeline(20);
        var mapped = arrivals.Select((at, i) => transport.Map(Source(unchecked((uint)(i * 8_670_000L)), at), 1)[0]).ToArray();
        Check(transport.Pictures == arrivals.Length && mapped.Zip(mapped.Skip(1)).All(pair =>
            unchecked(pair.Second - pair.First) is >= 12_500 and <= 200_000),
            "TCP burst retained zero/overlong samples, lost a picture, or leaked unreliable camera fields");
        Check(Math.Abs((long)unchecked(mapped[^1] - mapped[0]) - arrivals[^1]) < 1000,
            "TCP minimum advances accumulated instead of being repaid against absolute source time");
        var hub = new StreamHub("synthetic-rtp-quantization"); var (id, reader) = hub.Subscribe();
        var rtps = new List<uint>();
        foreach (uint us in mapped)
        {
            hub.PublishVideo(new(VideoCodec.H264, true, us, null, [0, 0, 0, 1, 0x65, 0x80]));
            Check(reader.TryRead(out var packet) && packet is HubVideo, "RTP quantization test lost a frame");
            rtps.Add(((HubVideo)packet!).RtpTs);
        }
        hub.Unsubscribe(id);
        Check(rtps.Zip(rtps.Skip(1)).All(pair => pair.Second / 90 - pair.First / 90 is >= 12 and <= 200),
            "StreamHub 90-kHz to native millisecond conversion produced zero/overlong sample duration");
        foreach (var bad in new[] { Source(0, 2_000_000), Source(0, 8_000_000), Source(0, 3_000_000) with { ArrivalTimestampFrequency = 999 } })
        {
            bool rejected = false;
            try { burst.Map(bad, 1); } catch (InvalidDataException) { rejected = true; }
            Check(rejected, "source-clock reversal, disconnect or changed frequency was accepted");
        }
        return Task.CompletedTask;
    }

    public static async Task RealNightEncoderAndAacPreservation()
    {
        if (Ffmpeg.ExePath is not { } ffmpeg) { Console.WriteLine("SKIP real night cadence: ffmpeg absent"); return; }
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var ct = deadline.Token;
        var (encoded, errors) = await Ffmpeg.RunAsync(ffmpeg,
            ["-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=20", "-frames:v", "80",
             "-an", "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency", "-threads", "1",
             "-x264-params", "keyint=20:min-keyint=20:scenecut=0:repeat-headers=1:aud=1", "-f", "h264", "pipe:1"],
            [], TimeSpan.FromSeconds(10), ct);
        Check(encoded.Length > 0 && errors.Length == 0, "night test source failed");
        var pictures = RtpAccessUnitSplitter.Split(VideoCodec.H264, encoded).Where(unit => unit.HasVcl).ToArray();
        Check(pictures.Length == 80, "night source has wrong picture count");
        var source = new StreamHub("synthetic-night-sub");
        var low = new StreamHub("synthetic-night-low");
        var sink = new ProtectBridgeHost.LowQualitySourceSink(source, low);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var converter = new LowQualityVideoTranscoder("synthetic-night", source, low, ct, 20, 256, 20, 160, 90);
        var running = converter.RunAsync(stop.Token);
        var (id, reader) = low.Subscribe();
        byte[] adts = [0xff, 0xf1, 0x60, 0x40, 0x01, 0x9f, 0xfc, 1, 2, 3, 4, 5]; // AAC-LC 16 kHz mono
        var output = new List<HubVideo>();
        var audio = new List<HubAudioAac>();
        Task? producer = null;
        Task? audioProducer = null;
        Task? resumed = null;
        using var produce = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var resume = CancellationTokenSource.CreateLinkedTokenSource(ct);
        try
        {
            producer = Task.Run(async () =>
            {
                for (int i = 0; !produce.IsCancellationRequested; i++)
                {
                    var picture = pictures[i % pictures.Length];
                    // Intentionally misleading camera PTS: actual arrivals are 12.5 fps.
                    sink.PublishVideo(new VideoFrame(VideoCodec.H264, picture.Keyframe, (uint)(i * 50_000), null,
                        RtpAccessUnitSplitter.AnnexB(picture.Nals)));
                    await Task.Delay(80, produce.Token);
                }
            }, ct);
            audioProducer = Task.Run(async () =>
            {
                while (!produce.IsCancellationRequested)
                { sink.PublishAac(new AacFrame(adts)); await Task.Delay(64, produce.Token); }
            }, ct);
            while (output.Count < 36)
            {
                Check(!running.IsCompleted, "night converter stopped unexpectedly");
                var packet = await reader.ReadAsync(ct);
                if (packet is HubVideo video) output.Add(video);
                if (packet is HubAudioAac aac) audio.Add(aac);
            }
            double videoSeconds = unchecked(output[^1].RtpTs - output[0].RtpTs) / 90000d;
            Check(videoSeconds > 2.65 && videoSeconds < 3.5, $"36 night frames retained a nominal20fps clock: {videoSeconds:F3}s");
            Check(output.Count == 36 && output.All(v => H26x.SplitNals(v.AnnexB).Any(nal => H26x.H265NalType(nal.Span) <= 31)),
                "real output omitted predictive pictures");
            Check(audio.Count > 30 && audio.All(a => a.Au.SequenceEqual(new byte[] { 1, 2, 3, 4, 5 }))
                && audio.Zip(audio.Skip(1)).All(pair => unchecked(pair.Second.RtpTs - pair.First.RtpTs) == 1024)
                && low.Audio is { IsAac: true, SampleRate: 16000, Channels: 1 }, "video timing change altered original AAC payload/sample clock");
            var (_, decodeErrors) = await Ffmpeg.RunAsync(ffmpeg,
                ["-hide_banner", "-loglevel", "error", "-threads", "1", "-f", "hevc", "-i", "pipe:0", "-an", "-f", "null", "pipe:1"],
                output.Select(v => v.AnnexB).ToArray(), TimeSpan.FromSeconds(10), ct);
            Check(decodeErrors.Length == 0, "night HEVC pictures no longer decode: " + decodeErrors);

            produce.Cancel();
            try { await producer; } catch (OperationCanceledException) { }
            try { await audioProducer; } catch (OperationCanceledException) { }
            sink.SourceStopped();
            long before = low.VideoFrameCount;
            await Task.Delay(350, ct);
            Check(low.VideoFrameCount == before && !low.LiveVideo && !low.HasBufferedGop, "old mapped source generation survived reconnect");
            while (reader.TryRead(out _)) { }
            resumed = Task.Run(async () =>
            {
                for (int i = 0; !resume.IsCancellationRequested; i++)
                {
                    var picture = pictures[i % pictures.Length];
                    sink.PublishVideo(new VideoFrame(VideoCodec.H264, picture.Keyframe, unchecked(3_000_000_000u + (uint)(i * 8_670_000)), null,
                        RtpAccessUnitSplitter.AnnexB(picture.Nals)));
                    await Task.Delay(50, resume.Token);
                }
            }, ct);
            var fresh = new List<HubVideo>();
            while (fresh.Count < 8)
            {
                var packet = await reader.ReadAsync(ct);
                if (packet is HubVideo video) fresh.Add(video);
            }
            double freshSeconds = unchecked(fresh[^1].RtpTs - fresh[0].RtpTs) / 90000d;
            Check(fresh.All(v => v.SourceEpoch == low.SourceEpoch) && fresh[0].Keyframe
                && freshSeconds is > 0.30 and < 0.50, "reconnected 20fps pictures used stale timestamps or jumped camera fields");
        }
        finally
        {
            stop.Cancel(); produce.Cancel(); resume.Cancel();
            if (producer != null) try { await producer; } catch (OperationCanceledException) { }
            if (audioProducer != null) try { await audioProducer; } catch (OperationCanceledException) { }
            if (resumed != null) try { await resumed; } catch (OperationCanceledException) { }
            await running.WaitAsync(TimeSpan.FromSeconds(4));
            low.Unsubscribe(id);
        }
    }

    public static async Task RealTcpBurstEncoderAndNativeMilliseconds()
    {
        if (Ffmpeg.ExePath is not { } ffmpeg) throw new InvalidOperationException("ffmpeg required for TCP burst regression");
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(25)); var ct = deadline.Token;
        var (encoded, errors) = await Ffmpeg.RunAsync(ffmpeg,
            ["-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=20", "-frames:v", "80",
             "-an", "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency", "-threads", "1",
             "-x264-params", "keyint=20:min-keyint=20:scenecut=0:repeat-headers=1:aud=1", "-f", "h264", "pipe:1"],
            [], TimeSpan.FromSeconds(10), ct);
        Check(errors.Length == 0, "TCP burst source could not encode");
        var pictures = RtpAccessUnitSplitter.Split(VideoCodec.H264, encoded).Where(unit => unit.HasVcl).ToArray();
        Check(pictures.Length == 80, "TCP burst source picture count changed");
        var clock = new GopAacContractTests.Clock();
        var source = new StreamHub("synthetic-tcp-source", clock); var low = new StreamHub("synthetic-tcp-low");
        source.PublishInfo(new(160, 90, 20));
        var sink = new ProtectBridgeHost.LowQualitySourceSink(source, low);
        sink.PublishVideo(new(VideoCodec.H264, true, 0, null, RtpAccessUnitSplitter.AnnexB(pictures[0].Nals)));
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var converter = new LowQualityVideoTranscoder("synthetic-tcp-burst", source, low, ct, 20, 256, 20, 160, 90);
        Task running = converter.RunAsync(stop.Token); var (id, reader) = low.Subscribe();
        var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
        using var sender = new TcpClient();
        Task? receive = null;
        try
        {
            for (int i = 0; source.SubscriberCount == 0 && i < 500; i++) await Task.Delay(2, ct);
            Check(source.SubscriberCount > 0, "TCP burst encoder did not subscribe");
            var accepted = listener.AcceptTcpClientAsync(ct).AsTask();
            await sender.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port, ct);
            using var receiver = await accepted;
            receive = Task.Run(async () =>
            {
                var stream = receiver.GetStream(); long previous = 0;
                for (int i = 1; i < pictures.Length; i++)
                {
                    byte[] header = new byte[12]; await stream.ReadExactlyAsync(header, ct);
                    long arrival = BinaryPrimitives.ReadInt64BigEndian(header);
                    int length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(8));
                    Check(length is > 0 and < 1_048_576, "TCP synthetic frame bound changed");
                    byte[] data = new byte[length]; await stream.ReadExactlyAsync(data, ct);
                    clock.Advance(TimeSpan.FromMicroseconds(arrival - previous)); previous = arrival;
                    sink.PublishVideo(new(VideoCodec.H264, pictures[i].Keyframe, unchecked((uint)(i * 8_670_000L)), null, data));
                    sink.PublishAac(new([0xff, 0xf1, 0x60, 0x40, 0x01, 0x9f, 0xfc, 1, 2, 3, 4, 5]));
                }
            }, ct);
            using var burst = new MemoryStream();
            for (int i = 1; i < pictures.Length; i++)
            {
                long arrival = i < 20 ? i * 50_000L : i < 60 ? 2_950_000L : 3_000_000L + (i - 60) * 50_000L;
                byte[] data = RtpAccessUnitSplitter.AnnexB(pictures[i].Nals); byte[] header = new byte[12];
                BinaryPrimitives.WriteInt64BigEndian(header, arrival); BinaryPrimitives.WriteInt32BigEndian(header.AsSpan(8), data.Length);
                burst.Write(header); burst.Write(data);
            }
            // One TCP write contains forty distinct pictures at one arrival stamp
            // after a two-second read stall. No camera or network device is used.
            await sender.GetStream().WriteAsync(burst.ToArray(), ct); await receive;
            var output = new List<HubVideo>(); var audio = new List<HubAudioAac>();
            while (output.Count < 70)
            {
                Check(!running.IsCompleted, "TCP burst encoder stopped unexpectedly");
                var packet = await reader.ReadAsync(ct);
                if (packet is HubVideo video) output.Add(video);
                if (packet is HubAudioAac aac) audio.Add(aac);
            }
            Check(output.Zip(output.Skip(1)).All(pair => pair.Second.RtpTs / 90 - pair.First.RtpTs / 90 is >= 12 and <= 200),
                "Real encoded TCP burst emitted zero/overlong native millisecond durations");
            Check(output.Zip(output.Skip(1)).All(pair => unchecked(pair.Second.RtpTs - pair.First.RtpTs) > 0),
                "Real TCP burst RTP clock stopped advancing");
            Check(audio.Count == 79 && audio.All(a => a.Au.SequenceEqual(new byte[] { 1, 2, 3, 4, 5 }))
                && audio.Zip(audio.Skip(1)).All(pair => unchecked(pair.Second.RtpTs - pair.First.RtpTs) == 1024),
                "TCP burst phase filter lost/changed original AAC units or sample clock");
            var (_, decodeErrors) = await Ffmpeg.RunAsync(ffmpeg,
                ["-hide_banner", "-loglevel", "error", "-threads", "1", "-f", "hevc", "-i", "pipe:0", "-an", "-f", "null", "pipe:1"],
                output.Select(v => v.AnnexB).ToArray(), TimeSpan.FromSeconds(10), ct);
            Check(decodeErrors.Length == 0, "TCP burst changed encoded predictive pictures: " + decodeErrors);
        }
        finally
        {
            stop.Cancel(); listener.Stop(); low.Unsubscribe(id);
            if (receive != null) try { await receive; } catch (OperationCanceledException) { }
            await running.WaitAsync(TimeSpan.FromSeconds(4));
        }
    }
}
