using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Neolink.Media;
using Neolink.Onvif;
using Neolink.Protocol;
using Neolink.Streaming;

internal static class StreamReliabilityTests
{
    private static readonly byte[] Video = [0,0,0,1,0x67,0x42,0,0x1e,0x80,0,0,0,1,0x68,0x80,0,0,0,1,0x65,0x80];
    private static readonly byte[] Aac = [0xff,0xf1,0x60,0x40,0x01,0x9f,0xfc,1,2,3,4,5];
    private static void Check(bool condition, string message) { if (!condition) throw new Exception(message); }
    private static void Publish(StreamHub hub, uint timestamp = 0) => hub.PublishVideo(new(VideoCodec.H264, true, timestamp, null, Video));

    public static async Task FreshDescriptionsAcrossEpochs()
    {
        var hub = new StreamHub("synthetic-epoch"); Publish(hub);
        Check(await hub.WaitForDescribeInfoAsync(TimeSpan.FromSeconds(3), default), "video-only source never completed its first audio probe");
        Check(hub.TryGetCurrentDescription(out var first) && first.Audio == null, "first description unexpectedly includes audio");
        hub.SourceStopped();
        Check(!hub.VideoReady && !hub.TryGetCurrentDescription(out _) && hub.Audio == null && hub.Sps == null,
            "disconnected source retained SDP metadata");
        Check(!await hub.WaitForDescribeInfoAsync(TimeSpan.FromMilliseconds(40), default), "offline source described successfully");
        Publish(hub, 1000);
        var waiting = hub.WaitForDescribeInfoAsync(TimeSpan.FromSeconds(2), default);
        await Task.Delay(60);
        Check(!waiting.IsCompleted, "reconnected source skipped its new audio probe");
        hub.PublishAac(new(Aac));
        Check(await waiting && hub.TryGetCurrentDescription(out var second) && second.SourceEpoch != first.SourceEpoch
            && second.Audio is { IsAac: true, SampleRate: 16000 }, "fresh AAC was missing from reconnected SDP");
        hub.TryGetCurrentDescription(out var copy);
        copy.Sps![0] ^= 0xff;
        Check(hub.Sps![0] == 0x67, "mutating an SDP snapshot changed publisher metadata");
        using var cancelled = new CancellationTokenSource(); cancelled.Cancel();
        try { await hub.WaitForDescribeInfoAsync(TimeSpan.FromSeconds(1), cancelled.Token); throw new Exception("caller cancellation ignored"); }
        catch (OperationCanceledException) { }
        var switching = new StreamHub("synthetic-codec-change");
        switching.PublishAac(new(Aac));
        switching.PublishVideo(new(VideoCodec.H264,false,0,null,[0,0,0,1,0x67,0x42,0,0x1e]));
        var oldWaiter = switching.WaitForDescribeInfoAsync(TimeSpan.FromSeconds(2),default);
        await Task.Delay(20);
        switching.PublishVideo(new(VideoCodec.H265,true,1000,null,
            [0,0,0,1,0x40,1,0x80,0,0,0,1,0x42,1,0x80,0,0,0,1,0x44,1,0x80,0,0,0,1,0x26,1,0x80]));
        Check(await oldWaiter.WaitAsync(TimeSpan.FromMilliseconds(300)) && switching.TryGetCurrentDescription(out var changed)
            && changed.Codec == VideoCodec.H265, "codec change stranded an existing description waiter");
    }

    public static async Task PublicationOrderAndByteBudgets()
    {
        var hub = new StreamHub("synthetic-publication-order");
        var (id, reader) = hub.Subscribe();
        try
        {
            await Task.WhenAll(Enumerable.Range(0, 500).Select(i => Task.Run(() =>
            { if ((i & 1) == 0) Publish(hub, (uint)i); else hub.PublishAac(new(Aac)); })));
            var indexes = new List<long>(); while (reader.TryRead(out var packet)) indexes.Add(packet.Index);
            Check(indexes.Count == 500 && indexes.Zip(indexes.Skip(1)).All(pair => pair.Second == pair.First + 1),
                "subscriber saw indices in a different order from published packets");
        }
        finally { hub.Unsubscribe(id); hub.SourceStopped(); }
        var queue = new ByteBoundedChannel<byte[]>(12, 4, bytes => bytes.Length);
        Check(queue.Writer.TryWrite([1,2,3,4]) && queue.Writer.TryWrite([5,6,7,8]), "valid raw chunks rejected");
        Check(queue.Reader.TryRead(out var first) && first.SequenceEqual(new byte[]{1,2,3,4}), "raw media changed");
        Check(queue.Writer.TryWrite([9,10,11,12,13,14,15,16]), "freed byte budget was not reusable");
        Check(!queue.Writer.TryWrite([17]) && queue.Diagnostics().Overflows == 1, "overflow silently retained or dropped bytes");
        Check(queue.Reader.TryRead(out var second) && second.SequenceEqual(new byte[]{5,6,7,8})
            && queue.Reader.TryRead(out var third) && third.Length == 8, "overflow damaged previously accepted raw chunks");
        try { await queue.Reader.Completion; throw new Exception("queue did not fail the preview after overflow"); }
        catch (QueueBudgetExceededException) { }
        Check(queue.Diagnostics().Bytes == 0 && queue.Diagnostics().PeakBytes == 12, "queue accounting did not match retained bytes");
    }

    public static async Task AlarmSubscriptionRecovery()
    {
        int attempts = 0; var states = new List<string>();
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await CameraService.WatchAlarmWithRetryAsync(_ =>
        {
            attempts++;
            if (attempts == 1) throw new CameraCommandException(31, 500);
            if (attempts == 2) throw new TimeoutException();
            stop.Cancel(); return Task.CompletedTask;
        }, (state, _) => states.Add(state), stop.Token, TimeSpan.FromMilliseconds(2));
        Check(attempts == 3 && states.Count(s => s == "retrying") == 2 && states.Last() == "stopped",
            "transient alarm failures did not recover independently");
        attempts = 0;
        await CameraService.WatchAlarmWithRetryAsync(_ => { attempts++; throw new CameraCommandException(31,404); },
            (state, _) => states.Add(state), default, TimeSpan.FromMilliseconds(1));
        Check(attempts == 1 && states.Last() == "unsupported", "unsupported alarms entered an endless command retry");
    }

    public static Task LowFrameRatesAndCertificatePinning()
    {
        for (int fps = 1; fps <= 4; fps++)
        {
            var timeline = new LowQualitySourceTimeline(fps);
            var output = Enumerable.Range(0, 12).Select(i => timeline.Map(new(i, [], true,0)
                { ArrivalTimestamp=i*1_000_000L/fps, ArrivalTimestampFrequency=1_000_000 },1)[0]).ToArray();
            Check(output.Zip(output.Skip(1)).All(pair => Math.Abs((long)(pair.Second-pair.First)-1_000_000L/fps) <= 2),
                $"valid {fps}-fps LQ was accelerated or unstable");
        }
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=synthetic-camera", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddMinutes(1));
        string pin = Convert.ToHexString(SHA256.HashData(certificate.RawData));
        Check(ReolinkHttpApi.CertificateMatches(certificate,pin) && !ReolinkHttpApi.CertificateMatches(certificate,new string('0',64))
            && !ReolinkHttpApi.CertificateMatches(null,pin), "camera pin accepted an unrelated or missing certificate");
        var diagnostics = LowQualityVideoTranscoder.ClassifyEncoderError("Permission denied at /synthetic/private; unknown encoder; SECRET-SYNTHETIC");
        Check(diagnostics.SequenceEqual(new[]{"device-inaccessible","encoder-unavailable"}), "encoder diagnostics leaked arbitrary process text");
        return Task.CompletedTask;
    }

    public static Task DurableReplayAndRecovery()
    {
        string folder = Path.Combine(Path.GetTempPath(), "synthetic-replay-"+Guid.NewGuid().ToString("N"));
        string path = Path.Combine(folder,"pushes.jsonl");
        ProtectEventBroker? broker = null, restored = null;
        try
        {
            broker = new ProtectEventBroker(replayPath:path,replayMaxBytes:131072);
            for (int i=0;i<300;i++) broker.Publish(new MotionPush("MD",["person"]));
            broker.Publish(new MotionPush("MD",["person"],External:true));
            var replay = JsonSerializer.SerializeToElement(broker.Replay(0,256));
            Check(replay.GetProperty("events").GetArrayLength() == 256 && replay.GetProperty("oldestSeq").GetInt64() < 45,
                "persistent replay retained only the RAM ring");
            string instance = replay.GetProperty("instanceId").GetString()!;
            long epoch = replay.GetProperty("source").GetProperty("connectionEpoch").GetInt64();
            try { using var duplicate = new ProtectReplayJournal(path,131072,TimeSpan.FromDays(1),DateTimeOffset.UtcNow); throw new Exception("duplicate writer allowed"); }
            catch (IOException) { }
            broker.Stop();
            File.AppendAllText(path,"{partial-last-append");
            restored = new ProtectEventBroker(replayPath:path,replayMaxBytes:131072);
            var recovered = JsonSerializer.SerializeToElement(restored.Replay(299));
            Check(recovered.GetProperty("instanceId").GetString() == instance && recovered.GetProperty("currentSeq").GetInt64() == 300
                && recovered.GetProperty("source").GetProperty("connectionEpoch").GetInt64() > epoch
                && !recovered.GetProperty("source").GetProperty("connected").GetBoolean(), "restart lost durable cursor or source boundary");
            restored.Publish(new MotionPush("none",[],false));
            Check(JsonSerializer.SerializeToElement(restored.Replay(300)).GetProperty("events")[0].GetProperty("seq").GetInt64() == 301,
                "journal sequence restarted instead of continuing");
            restored.Stop();
            Check(new FileInfo(path).Length <= 131072, "persistent replay exceeded configured byte budget");
            string boundedPath = Path.Combine(folder,"bounded.jsonl");
            using var bounded = new ProtectReplayJournal(boundedPath,65536,TimeSpan.FromMinutes(1),DateTimeOffset.UtcNow);
            var now = DateTimeOffset.UtcNow;
            for (int i=1;i<=1000;i++) bounded.Append(new(i,now,true,["person"],["person"],"MD",1),now);
            Check(bounded.Entries(now).Length < 1000 && new FileInfo(boundedPath).Length <= 65536,
                "persistent replay failed byte retention");
            Check(bounded.Entries(now.AddMinutes(2)).Length == 0 && bounded.Sequence == 1000,
                "time retention erased the cursor or retained expired pushes");
            bounded.Append(new(1001,now.AddMinutes(2),true,["person"],[],"MD",1),now.AddMinutes(2));
            var bytesBefore = new FileInfo(boundedPath).Length;
            bounded.Dispose(); bounded.Entries(now.AddDays(1));
            Check(new FileInfo(boundedPath).Length == bytesBefore, "reading a disposed journal reopened its writer");
            try { bounded.Append(new(1002,now,true,["person"],[],"MD",1),now); throw new Exception("disposed journal accepted writes"); }
            catch (ObjectDisposedException) { }
            var unicode = new ProtectEventBroker();
            for (int i=0;i<256;i++) unicode.Publish(new("MD",Enumerable.Repeat(new string('\u4e2d',64),16).ToArray()));
            byte[] page = JsonSerializer.SerializeToUtf8Bytes(unicode.Replay(0,256));
            Check(page.Length <= 512000 && JsonSerializer.Deserialize<JsonElement>(page).GetProperty("events").GetArrayLength() > 0,
                "bounded replay cannot be collected within the adapter byte budget");
            unicode.Stop();
        }
        finally
        {
            broker?.Stop(); restored?.Stop();
            Check(Path.GetFullPath(folder).StartsWith(Path.Combine(Path.GetTempPath(), "synthetic-replay-"), StringComparison.Ordinal),
                "refusing cleanup outside the synthetic replay scope");
            if (Directory.Exists(folder)) Directory.Delete(folder,true);
        }
        return Task.CompletedTask;
    }
}
