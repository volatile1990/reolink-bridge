using System.Diagnostics;
using Neolink.Media;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class LowQualityVideoTranscoderTests
{
    private static byte[] Annex(params byte[][] nals) =>
        nals.SelectMany(nal => new byte[] { 0, 0, 0, 1 }.Concat(nal)).ToArray();
    private static void Check(bool condition, string message) { if (!condition) throw new Exception(message); }

    public static Task IncrementalHevcBoundaries()
    {
        byte[] vps = [0x40, 0x01, 0x80], sps = [0x42, 0x01, 0x80], pps = [0x44, 0x01, 0x80];
        byte[] aud = [0x46, 0x01, 0x80], key = [0x26, 0x01, 0x80, 0x23], first = [0x02, 0x01, 0x80, 0x24];
        byte[] continuation = [0x02, 0x01, 0x40, 0x25], suffix = [0x50, 0x01, 0x80];
        byte[] stream = Annex(aud, vps, sps, pps, key, continuation, suffix, aud, first, aud, first);
        for (int chunk = 1; chunk <= stream.Length; chunk++)
        {
            var reader = new HevcAccessUnitReader();
            var frames = new List<HevcAccessUnit>();
            for (int offset = 0; offset < stream.Length; offset += chunk)
                frames.AddRange(reader.Feed(stream.AsSpan(offset, Math.Min(chunk, stream.Length - offset))));
            frames.AddRange(reader.Feed([], endOfStream: true));
            Check(frames.Count == 3 && frames[0].Keyframe && !frames[1].Keyframe && !frames[2].Keyframe,
                $"pipe chunk size {chunk} changed HEVC picture boundaries/keyframe flags");
            Check(H26x.SplitNals(frames[0].Data).Select(n => H26x.H265NalType(n.Span))
                .SequenceEqual([35, 32, 33, 34, 19, 1, 40]), "HEVC reader lost parameters, continuation or suffix SEI");
        }
        var onlyParameters = new HevcAccessUnitReader();
        Check(onlyParameters.Feed(Annex(vps, sps, pps), endOfStream: true).Count == 0, "parameters invented a picture");
        bool rejected = false;
        try { new HevcAccessUnitReader().Feed(new byte[2 * 1024 * 1024 + 1]); }
        catch (InvalidDataException) { rejected = true; }
        Check(rejected, "untrusted ffmpeg output has an unbounded memory path");
        return Task.CompletedTask;
    }

    public static Task EncoderIsolation()
    {
        var info = LowQualityVideoTranscoder.StartInfo("synthetic-ffmpeg", VideoCodec.H264, 10, 384, 20);
        var args = info.ArgumentList.ToArray();
        Check(!info.UseShellExecute && info.RedirectStandardInput && info.RedirectStandardOutput && info.RedirectStandardError,
            "low-quality encoder must use isolated pipes");
        Check(args.Count(a => a == "pipe:0") == 1 && args.Count(a => a == "pipe:1") == 1
            && !args.Any(a => a.Contains("rtsp://", StringComparison.OrdinalIgnoreCase)), "encoder command exposes a camera URL");
        Check(args.Contains("libx265") && args.Contains("fps=10") && args.Contains("-an")
            && args.Contains("pools=none:frame-threads=1:wpp=0:repeat-headers=1:aud=1:bframes=0:rc-lookahead=0:scenecut=0:keyint=10:min-keyint=10:log-level=error"),
            "CPU bounds, output rate, decoder headers or video-only processing regressed");
        return Task.CompletedTask;
    }

    public static Task HardwareAndScaledEncoderIsolation()
    {
        var hardware = LowQualityVideoTranscoder.StartInfo("synthetic-ffmpeg", VideoCodec.H264, 20, 1024, 20,
            896, 512, "vaapi", "/dev/dri/renderD128");
        var args = hardware.ArgumentList.ToArray();
        Check(args.Contains("vaapi=low:/dev/dri/renderD128") && hardware.Environment["LIBVA_DRIVER_NAME"] == "iHD"
            && args.Contains("fps=20,scale=896:512:flags=fast_bilinear,setsar=1,format=nv12,hwupload")
            && args.Contains("hevc_vaapi") && !args.Contains("libx265") && !args.Contains("-low_power"),
            "explicit Intel HEVC hardware selection can fall back or skip upload/scaling");
        string Value(string option) => args[Array.IndexOf(args, option) + 1];
        Check(Value("-bf") == "0" && Value("-g") == "20" && Value("-idr_interval") == "0"
            && Value("-aud") == "0" && Value("-async_depth") == "1" && Value("-flags:v") == "-global_header"
            && Value("-bsf:v") == "dump_extra=freq=keyframe",
            "hardware output lacks decoder-friendly GOP boundaries or bounded buffering");
        Check(args.Count(a => a == "pipe:0") == 1 && args.Count(a => a == "pipe:1") == 1
            && !args.Any(a => a.Contains("rtsp://", StringComparison.OrdinalIgnoreCase)), "hardware encoder exposes camera access");
        var software = LowQualityVideoTranscoder.StartInfo("synthetic-ffmpeg", VideoCodec.H265, 10, 384, 20, 160, 90);
        Check(software.ArgumentList.Contains("fps=10,scale=160:90:flags=fast_bilinear,setsar=1")
            && software.ArgumentList.Contains("libx265") && !software.ArgumentList.Contains("hevc_vaapi"),
            "software scaling changed the selected backend");
        foreach (var (encoder, device, width, height) in new[]
        {
            ("auto", "/dev/dri/renderD128", 160, 90), ("vaapi", "/tmp/device", 160, 90),
            ("vaapi", "/dev/dri/renderD128,driver=other", 160, 90), ("software", "/dev/dri/renderD128", 159, 90),
            ("software", "/dev/dri/renderD128", 160, 0)
        })
        {
            bool rejected = false;
            try { LowQualityVideoTranscoder.StartInfo("synthetic-ffmpeg", VideoCodec.H264, 10, 384, 20, width, height, encoder, device); }
            catch (ArgumentException) { rejected = true; }
            Check(rejected, "unsafe backend, device expression or resolution was accepted");
        }
        return Task.CompletedTask;
    }

    /// <summary>Explicit GPU-only regression: count real pictures, including predictive frames and fragmented Annex-B.</summary>
    public static async Task HardwareFfmpegPictureCompleteness()
    {
        string ffmpeg = Ffmpeg.ExePath ?? throw new InvalidOperationException("GPU regression requires ffmpeg");
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var ct = deadline.Token;
        var (source, sourceErrors) = await Ffmpeg.RunAsync(ffmpeg,
            ["-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=896x512:rate=20",
             "-frames:v", "120", "-an", "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
             "-threads", "1", "-x264-params", "keyint=20:min-keyint=20:scenecut=0:repeat-headers=1:aud=1",
             "-f", "h264", "pipe:1"], [], TimeSpan.FromSeconds(15), ct);
        Check(source.Length > 0 && sourceErrors.Length == 0
            && RtpAccessUnitSplitter.Split(VideoCodec.H264, source).Count(u => u.HasVcl) == 120,
            "GPU regression source did not contain 120 actual H264 pictures");
        foreach (int fps in new[] { 10, 20 })
        {
            var settings = LowQualityVideoTranscoder.StartInfo(ffmpeg, VideoCodec.H264, fps, 1024, 20,
                896, 512, "vaapi", "/dev/dri/renderD128");
            var (encoded, encodeErrors) = await Ffmpeg.RunAsync(ffmpeg, settings.ArgumentList.ToArray(), [source],
                TimeSpan.FromSeconds(20), ct);
            Check(encoded.Length > 0 && encodeErrors.Length == 0, "actual VAAPI encoder failed: " + encodeErrors);
            int expected = 120 * fps / 20;
            var pictures = RtpAccessUnitSplitter.Split(VideoCodec.H265, encoded).Where(unit => unit.HasVcl).ToArray();
            Check(pictures.Length == expected && pictures.Count(unit => !unit.Keyframe) >= expected - 7,
                $"VAAPI {fps}fps emitted {pictures.Length}/{expected} actual HEVC pictures; predictive frames disappeared");
            foreach (int chunkSize in new[] { 257, 4096, 16384 })
            {
                var reader = new HevcAccessUnitReader();
                var frames = new List<HevcAccessUnit>();
                for (int offset = 0; offset < encoded.Length; offset += chunkSize)
                    frames.AddRange(reader.Feed(encoded.AsSpan(offset, Math.Min(chunkSize, encoded.Length - offset))));
                frames.AddRange(reader.Feed([], endOfStream: true));
                Check(frames.Count == expected && frames[0].Keyframe,
                    $"VAAPI {fps}fps incremental parser emitted {frames.Count}/{expected} pictures at chunk {chunkSize}");
                foreach (var key in frames.Where(frame => frame.Keyframe))
                    LowQualityVideoTranscoder.CheckFirstOutput(H26x.SplitNals(key.Data), 896, 512, true);
            }
            var (progress, decodeErrors) = await Ffmpeg.RunAsync(ffmpeg,
                ["-hide_banner", "-loglevel", "error", "-threads", "1", "-f", "hevc", "-i", "pipe:0",
                 "-an", "-f", "null", "-progress", "pipe:1", "pipe:2"], [encoded], TimeSpan.FromSeconds(15), ct);
            string text = System.Text.Encoding.UTF8.GetString(progress);
            Check(decodeErrors.Length == 0 && text.Split('\n').Select(line => line.Trim()).Contains("frame=" + expected),
                $"VAAPI {fps}fps did not decode all {expected} pictures: " + decodeErrors);
            Console.WriteLine($"PASS GPU VAAPI {fps}fps: {expected} actual encoded/assembled/decoded pictures including P frames");
        }
    }

    public static Task RealFfmpegReconnectAndShutdown() => RealFfmpegReconnectAndShutdownCore(scaled: false);
    public static Task RealFfmpegScaledReconnectAndShutdown() => RealFfmpegReconnectAndShutdownCore(scaled: true);

    private static async Task RealFfmpegReconnectAndShutdownCore(bool scaled)
    {
        if (Ffmpeg.ExePath is not { } ffmpeg)
        {
            Console.WriteLine("SKIP low-quality real encoder check: ffmpeg is absent");
            return;
        }
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var ct = deadline.Token;
        var (h264, error) = await Ffmpeg.RunAsync(ffmpeg,
            ["-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=20",
                "-frames:v", "80", "-an", "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
                "-threads", "1", "-x264-params", "keyint=20:min-keyint=20:scenecut=0:repeat-headers=1:aud=1",
                "-f", "h264", "pipe:1"], [], TimeSpan.FromSeconds(10), ct);
        Check(h264.Length > 0 && error.Length == 0, "synthetic H264 source could not be encoded: " + error);
        var pictures = RtpAccessUnitSplitter.Split(VideoCodec.H264, h264).Where(u => u.HasVcl).ToArray();
        Check(pictures.Length == 80 && pictures[0].Keyframe, "synthetic source lacks expected decoder pictures");

        var main = new StreamHub("synthetic-main-unchanged");
        main.PublishVideo(new VideoFrame(VideoCodec.H264, true, 0, null, RtpAccessUnitSplitter.AnnexB(pictures[0].Nals)));
        var mainBefore = main.GetVideoBufferDiagnostics();
        var source = new StreamHub("synthetic-sub-source");
        var low = new StreamHub("synthetic-low-output");
        int expectedWidth = scaled ? 160 : 320, expectedHeight = scaled ? 90 : 180;
        var transcoder = new LowQualityVideoTranscoder("synthetic-low", source, low, ct, 10, 256, 20,
            scaled ? expectedWidth : 0, scaled ? expectedHeight : 0);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var running = transcoder.RunAsync(stop.Token);
        var outputPackets = new List<HubVideo>();
        var (subscriber, reader) = low.Subscribe();
        Task? producer = null;
        CancellationTokenSource? producing = null;
        try
        {
            async Task Produce(CancellationToken producerCt)
            {
                uint timestamp = 0;
                while (!producerCt.IsCancellationRequested)
                    foreach (var picture in pictures)
                    {
                        producerCt.ThrowIfCancellationRequested();
                        source.PublishVideo(new VideoFrame(VideoCodec.H264, picture.Keyframe, timestamp, null,
                            RtpAccessUnitSplitter.AnnexB(picture.Nals)));
                        // Synthetic AAC AU: it is ignored by the video encoder, but its indices must
                        // remain continuous through cached-GOP priming and the live source subscription.
                        source.PublishAac(new AacFrame([0xff, 0xf1, 0x50, 0x40, 0x01, 0x9f, 0xfc, 1, 2, 3, 4, 5]));
                        timestamp = unchecked(timestamp + 50_000);
                        await Task.Delay(50, producerCt);
                    }
            }
            async Task WaitForFrames(long count)
            {
                using var wait = CancellationTokenSource.CreateLinkedTokenSource(ct);
                wait.CancelAfter(TimeSpan.FromSeconds(9));
                while (low.VideoFrameCount < count)
                {
                    Check(!running.IsCompleted, "low encoder ended the bridge service during normal operation");
                    await Task.Delay(25, wait.Token);
                }
            }
            producing = CancellationTokenSource.CreateLinkedTokenSource(ct);
            producer = Produce(producing.Token);
            await WaitForFrames(8);
            Check(low.Codec == VideoCodec.H265 && low.VideoReady && low.Width == expectedWidth && low.Height == expectedHeight,
                $"real encoder did not publish decodable low-resolution HEVC parameters: {low.Codec}/{low.VideoReady}/{low.Width}x{low.Height}");
            Check(low.Sps != null && H26x.TryGetDimensions(VideoCodec.H265, low.Sps, out var parsedWidth, out var parsedHeight)
                && parsedWidth == expectedWidth && parsedHeight == expectedHeight,
                "HEVC SPS did not independently prove the encoded low dimensions/conformance crop");
            var diagnostics = System.Text.Json.JsonSerializer.SerializeToElement(transcoder.Diagnostics());
            Check(diagnostics.GetProperty("state").GetString() == "active"
                && diagnostics.GetProperty("encoder").GetString() == "libx265"
                && !diagnostics.GetProperty("softwareFallback").GetBoolean(), "live encoder diagnostics misreport the backend");
            while (reader.TryRead(out var packet)) if (packet is HubVideo video) outputPackets.Add(video);
            Check(outputPackets.Count >= 8 && outputPackets[0].Keyframe, "low stream was not keyframe-first");
            var firstParameters = H26x.SplitNals(outputPackets[0].AnnexB);
            LowQualityVideoTranscoder.CheckFirstOutput(firstParameters, (uint)expectedWidth, (uint)expectedHeight, true);
            bool mismatched = false;
            try { LowQualityVideoTranscoder.CheckFirstOutput(firstParameters, (uint)expectedWidth + 2, (uint)expectedHeight, true); }
            catch (InvalidDataException) { mismatched = true; }
            Check(mismatched, "encoder accepted output with mismatched dimensions");
            for (int i = 1; i < outputPackets.Count; i++)
                Check(unchecked(outputPackets[i].RtpTs - outputPackets[i - 1].RtpTs) == 9000,
                    "10 fps low output did not keep a 90 kHz RTP clock");
            Check(low.Audio == null, "video-only transcoder invented or duplicated an audio track");
            var (decoded, decodeErrors) = await Ffmpeg.RunAsync(ffmpeg,
                ["-hide_banner", "-loglevel", "error", "-threads", "1", "-f", "hevc", "-i", "pipe:0", "-an", "-f", "null", "pipe:1"],
                outputPackets.Select(v => v.AnnexB).ToArray(), TimeSpan.FromSeconds(10), ct);
            Check(decoded.Length == 0 && decodeErrors.Length == 0, "transcoded HEVC failed actual decoding: " + decodeErrors);

            producing.Cancel();
            try { await producer; } catch (OperationCanceledException) { }
            source.SourceStopped();
            long previousFrames = low.VideoFrameCount;
            await Task.Delay(600, ct);
            Check(!low.HasBufferedGop && !low.LiveVideo && low.VideoFrameCount == previousFrames,
                "old encoder generation kept publishing/caching after upstream disconnect");
            producing.Dispose();
            producing = CancellationTokenSource.CreateLinkedTokenSource(ct);
            producer = Produce(producing.Token);
            await WaitForFrames(previousFrames + 5);
            Check(low.HasBufferedGop && low.LiveVideo, "low stream did not recover from source reconnect");
            var mainAfter = main.GetVideoBufferDiagnostics();
            Check(mainAfter.TotalAccessUnits == mainBefore.TotalAccessUnits && mainAfter.GopBytes == mainBefore.GopBytes
                && mainAfter.KeyframeCount == mainBefore.KeyframeCount && main.VideoFrameCount == 1 && main.SourceEpoch == 0,
                "low encoder changed the high-quality stream");
            var elapsed = Stopwatch.StartNew();
            stop.Cancel();
            await running.WaitAsync(TimeSpan.FromSeconds(4));
            Check(elapsed.Elapsed < TimeSpan.FromSeconds(4) && !low.LiveVideo && !low.HasBufferedGop,
                "low encoder shutdown left stale output or a blocked subprocess");
        }
        finally
        {
            stop.Cancel(); producing?.Cancel();
            if (producer != null) try { await producer; } catch (OperationCanceledException) { }
            try { await running.WaitAsync(TimeSpan.FromSeconds(4)); } catch (OperationCanceledException) { }
            producing?.Dispose(); low.Unsubscribe(subscriber);
        }
    }
}
