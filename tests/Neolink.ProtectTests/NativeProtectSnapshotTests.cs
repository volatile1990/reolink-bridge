using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using System.Xml.Linq;
using Neolink.Bc;
using Neolink.Bc.Xml;
using Neolink.Protocol;
using Neolink.Streaming;

internal static class NativeProtectSnapshotTests
{
    private static void Check(bool condition, string message) { if (!condition) throw new Exception(message); }

    // Synthetic marker/header fixture; validates the bounded JPEG contract,
    // not successful decoding of entropy data or a real camera image.
    private static byte[] Jpeg(byte tag = 0x21) => new byte[]
    {
        0xFF, 0xD8, 0xFF, 0xC0, 0, 17, 8, 0, 16, 0, 32, 3,
        1, 0x11, 0, 2, 0x11, 1, 3, 0x11, 1,
        0xFF, 0xDA, 0, 12, 3, 1, 0, 2, 0x11, 3, 0x11, 0, 63, 0
    }.Concat(Enumerable.Repeat(tag, 128)).Concat(new byte[] { 0xFF, 0xD9 }).ToArray();

    public static async Task CacheAndFreshness()
    {
        var clock = new ManualTimeProvider(); long epoch = 0; bool ready = true;
        var first = new Camera(_ => Task.FromResult<byte[]?>(Jpeg()));
        var second = new Camera(_ => Task.FromResult<byte[]?>(Jpeg(0x22)));
        IBcCamera? current = first;
        var provider = new NativeProtectSnapshotProvider(() => current, () => ready, () => epoch, clock);
        var a = await provider.GetJpegAsync(CancellationToken.None);
        Check(a != null && first.Calls == 1, "native capture did not use the existing camera");
        clock.Advance(TimeSpan.FromMilliseconds(4999));
        Check(ReferenceEquals(a, await provider.GetJpegAsync(CancellationToken.None)) && first.Calls == 1,
            "fresh five-second cache polled the camera again");
        clock.Advance(TimeSpan.FromMilliseconds(1));
        await provider.GetJpegAsync(CancellationToken.None);
        Check(first.Calls == 2, "expired cache was not refreshed at the exact boundary");
        ready = false;
        Check(await provider.GetJpegAsync(CancellationToken.None) == null && first.Calls == 2,
            "stale/offline source served a cached image or polled the camera");
        ready = true; await provider.GetJpegAsync(CancellationToken.None);
        Check(first.Calls == 3, "stale source did not invalidate its old cached image");
        epoch++; await provider.GetJpegAsync(CancellationToken.None);
        Check(first.Calls == 4, "publisher epoch change retained the old snapshot");
        current = second;
        Check((await provider.GetJpegAsync(CancellationToken.None))!.SequenceEqual(Jpeg(0x22)) && second.Calls == 1,
            "new live session received the old session's cached image");
        current = null;
        Check(await provider.GetJpegAsync(CancellationToken.None) == null, "offline session returned an old image");
    }

    public static async Task SharedCaptureCancellation()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<byte[]?>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken captureToken = default;
        var camera = new Camera(async ct => { captureToken = ct; entered.TrySetResult(); return await release.Task.WaitAsync(ct); });
        var provider = new NativeProtectSnapshotProvider(() => camera, () => true);
        using var cancelled = new CancellationTokenSource();
        var one = provider.GetJpegAsync(cancelled.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(1));
        var others = Enumerable.Range(0, 8).Select(_ => provider.GetJpegAsync(CancellationToken.None)).ToArray();
        cancelled.Cancel(); bool aborted = false;
        try { await one; } catch (OperationCanceledException) { aborted = true; }
        Check(aborted && !captureToken.IsCancellationRequested && camera.Calls == 1,
            "one cancelled HTTP caller cancelled shared work or duplicated captures");
        release.TrySetResult(Jpeg());
        Check((await Task.WhenAll(others)).All(j => j != null) && camera.Calls == 1,
            "concurrent snapshot requests did not share exactly one native command");
    }

    public static async Task InvalidAndChangedSession()
    {
        var clock = new ManualTimeProvider(); bool ready = true; long epoch = 0;
        byte[]? candidate = Jpeg(); candidate[^1] = 0;
        var camera = new Camera(_ => Task.FromResult<byte[]?>(candidate));
        var provider = new NativeProtectSnapshotProvider(() => camera, () => ready, () => epoch, clock);
        Check(await provider.GetJpegAsync(CancellationToken.None) == null, "truncated JPEG was cached");
        Check(await provider.GetJpegAsync(CancellationToken.None) == null && camera.Calls == 1,
            "invalid native reply caused repeated immediate commands");
        candidate = Jpeg(); clock.Advance(TimeSpan.FromSeconds(15));
        Check(await provider.GetJpegAsync(CancellationToken.None) != null && camera.Calls == 2,
            "failure cooldown never allowed a later valid capture");
        byte[] badSof = Jpeg(); badSof[8] = badSof[9] = 0;
        Check(!NativeProtectSnapshotProvider.IsValidJpeg(badSof), "zero-height SOF was accepted");
        byte[] emptyScan = Jpeg(); emptyScan[24] = 6; emptyScan[25] = 0;
        Check(!NativeProtectSnapshotProvider.IsValidJpeg(emptyScan), "zero-component SOS was accepted");
        byte[] wrongScanLength = Jpeg(); wrongScanLength[24] = 10;
        Check(!NativeProtectSnapshotProvider.IsValidJpeg(wrongScanLength), "SOS length did not match its component count");
        byte[] excessScanComponents = Jpeg(); excessScanComponents[24] = 14; excessScanComponents[25] = 4;
        Check(!NativeProtectSnapshotProvider.IsValidJpeg(excessScanComponents), "SOS had more components than SOF");
        Check(!NativeProtectSnapshotProvider.IsValidJpeg(new byte[] { 0xFF, 0xD8 }.Concat(new byte[128]).Concat(new byte[] { 0xFF, 0xD9 }).ToArray()),
            "SOI/EOI alone counted as a structurally valid JPEG");
        Check(!NativeProtectSnapshotProvider.IsValidJpeg(new byte[NativeProtectSnapshotProvider.MaxJpegBytes + 1]), "JPEG byte cap ignored");
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<byte[]?>(TaskCreationOptions.RunContinuationsAsynchronously);
        camera.Capture = async ct => { entered.TrySetResult(); return await release.Task.WaitAsync(ct); };
        epoch++; var oldCapture = provider.GetJpegAsync(CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(1)); epoch++;
        release.TrySetResult(Jpeg());
        Check(await oldCapture == null, "capture spanning a publisher epoch change was served");
        camera.Capture = _ => Task.FromResult<byte[]?>(Jpeg(0x23));
        Check((await provider.GetJpegAsync(CancellationToken.None))!.SequenceEqual(Jpeg(0x23)),
            "invalid old capture poisoned recovery on the current publisher");
    }

    public static async Task TotalBudgetAndLifetime()
    {
        var started = System.Diagnostics.Stopwatch.StartNew();
        var camera = new Camera(async ct => { await Task.Delay(Timeout.InfiniteTimeSpan, ct); return null; });
        var provider = new NativeProtectSnapshotProvider(() => camera, () => true);
        Check(await provider.GetJpegAsync(CancellationToken.None) == null,
            "unresponsive native command did not fail closed");
        Check(started.Elapsed is var elapsed && elapsed >= TimeSpan.FromSeconds(2.8) && elapsed < TimeSpan.FromSeconds(4.5),
            "three-second total snapshot budget was not enforced");
        using var stop = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        camera = new Camera(async ct => { entered.TrySetResult(); await Task.Delay(Timeout.InfiniteTimeSpan, ct); return null; });
        provider = new NativeProtectSnapshotProvider(() => camera, () => true, lifetime: stop.Token);
        var pending = provider.GetJpegAsync(CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(1)); stop.Cancel();
        Check(await pending.WaitAsync(TimeSpan.FromSeconds(1)) == null, "host shutdown left a native command waiting");
    }

    public static async Task WireReassemblyAndLimits()
    {
        byte[] jpeg = Jpeg();
        foreach (ushort finalStatus in new ushort[] { 200, 201 })
            await Wire(async (stream, context, stop) =>
            {
                var request = await BcCodec.ReadMessageAsync(stream, context, stop);
                AssertSnap(request);
                await Reply(stream, request, size: jpeg.Length, ct: stop);
                await Reply(stream, request, bytes: jpeg[..60], ct: stop);
                await Reply(stream, request, bytes: jpeg[60..], ct: stop, responseCode: finalStatus);
            }, async (camera, stop) =>
            {
                Check((await camera.SnapBoundedAsync(4 * 1024 * 1024, stop))!.SequenceEqual(jpeg),
                    "native BC chunks did not reassemble into the original JPEG");
            });
        foreach (bool announced in new[] { true, false })
            await Wire(async (stream, context, stop) =>
            {
                var request = await BcCodec.ReadMessageAsync(stream, context, stop); AssertSnap(request);
                if (announced) await Reply(stream, request, size: jpeg.Length, ct: stop);
                await Reply(stream, request, bytes: jpeg[..60], ct: stop, responseCode: 201);
            }, async (camera, stop) =>
            {
                bool rejected = false;
                try { await camera.SnapBoundedAsync(1024, stop); } catch (BcProtocolException) { rejected = true; }
                Check(rejected, "201 accepted an incomplete or unannounced snapshot");
            });
        await Wire(async (stream, context, stop) =>
        {
            var request = await BcCodec.ReadMessageAsync(stream, context, stop); AssertSnap(request, "subStream");
            await Reply(stream, request, bytes: jpeg, ct: stop);
        }, async (camera, stop) =>
            Check((await camera.SnapAsync(stop))!.SequenceEqual(jpeg), "legacy snapshot behavior changed"));
        foreach (bool finalBytes in new[] { true, false })
        {
            const ushort snapshotNumber = 43, videoNumber = 42;
            var context = new BcContext(new EncryptionState());
            async Task<BcMessage> Parse(BcMessage message)
            {
                using var wire = new MemoryStream(BcCodec.Serialize(message, context.Encryption));
                return await BcCodec.ReadMessageAsync(wire, context, CancellationToken.None);
            }
            BcMessage Binary(uint messageId, ushort number, ushort status, byte[]? bytes) => new()
            {
                Meta = new BcMeta { MsgId = messageId, MsgNum = number,
                    Class = BcConstants.ClassModernZero, ResponseCode = status },
                Extension = bytes == null ? null : new ExtensionXml { BinaryData = 1, ChannelId = 0 },
                Binary = bytes
            };
            await Parse(Binary(BcConstants.MsgIdVideo, videoNumber, 200, [1, 2, 3]));
            await Parse(Binary(BcConstants.MsgIdSnap, snapshotNumber, 200, jpeg[..60]));
            Check(context.InBinMode.Contains(snapshotNumber), "multipart 200 released snapshot mode too early");
            byte[]? tail = finalBytes ? jpeg[60..] : null;
            var final = await Parse(Binary(BcConstants.MsgIdSnap, snapshotNumber, 201, tail));
            Check(tail == null ? final.Binary == null : final.Binary!.SequenceEqual(tail),
                "snapshot mode was cleared before the final 201 bytes were parsed");
            Check(!context.InBinMode.Contains(snapshotNumber) && context.InBinMode.Contains(videoNumber),
                "snapshot completion retained its mode or cleared the active video mode");
            var acknowledgement = await Parse(BcMessage.FromXml(new BcMeta { MsgId = BcConstants.MsgIdSnap,
                MsgNum = snapshotNumber, Class = BcConstants.ClassModernZero, ResponseCode = 200 },
                BcXmlBody.FromRaw(new XElement("Snap", new XElement("pictureSize", jpeg.Length)))));
            Check(acknowledgement.Xml?.RawElement("Snap")?.Element("pictureSize")?.Value == jpeg.Length.ToString()
                && acknowledgement.Binary == null, "reused snapshot number misclassified its XML acknowledgement");
            Check(context.InBinMode.Contains(videoNumber), "reused snapshot acknowledgement changed another video mode");
        }
        foreach (bool advertised in new[] { true, false })
            await Wire(async (stream, context, stop) =>
            {
                var request = await BcCodec.ReadMessageAsync(stream, context, stop); AssertSnap(request);
                await Reply(stream, request, size: advertised ? 65 : null, bytes: advertised ? null : new byte[65], ct: stop);
            }, async (camera, stop) =>
            {
                bool rejected = false;
                try { await camera.SnapBoundedAsync(64, stop); } catch (BcProtocolException) { rejected = true; }
                Check(rejected, "oversized advertised/binary snapshot exceeded the reassembly cap");
            });
    }

    public static async Task WireCancelledAndLateReply()
    {
        byte[] old = Jpeg(0x31), fresh = Jpeg(0x32);
        await Wire(async (stream, context, stop) =>
        {
            var first = await BcCodec.ReadMessageAsync(stream, context, stop); AssertSnap(first);
            await Reply(stream, first, size: old.Length, bytes: null, ct: stop);
            await Reply(stream, first, bytes: old[..32], ct: stop);
            var second = await BcCodec.ReadMessageAsync(stream, context, stop); AssertSnap(second);
            Check(second.Meta.MsgNum != first.Meta.MsgNum, "snapshot request did not advance its message number");
            await Reply(stream, first, bytes: old[32..], ct: stop);
            await Reply(stream, second, size: fresh.Length, ct: stop);
            await Reply(stream, second, bytes: fresh, ct: stop);
        }, async (camera, stop) =>
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(stop);
            deadline.CancelAfter(TimeSpan.FromMilliseconds(100)); bool cancelled = false;
            try { await camera.SnapBoundedAsync(1024, deadline.Token); } catch (OperationCanceledException) { cancelled = true; }
            Check(cancelled, "truncated multi-message native snapshot ignored total cancellation");
            Check((await camera.SnapBoundedAsync(1024, stop))!.SequenceEqual(fresh),
                "late cancelled snapshot bytes contaminated the next request");
        });
    }

    public static async Task WireFullAesSnapshot()
    {
        const string nonce = "SYNTHETIC-SNAPSHOT-NONCE", password = "synthetic-snapshot-key";
        byte[] jpeg = Jpeg();
        await Wire(async (stream, context, stop) =>
        {
            var upgrade = await BcCodec.ReadMessageAsync(stream, context, stop);
            Check(upgrade.Meta.MsgId == 1, "fixture requires one established login");
            context.Encryption.Set(EncryptionKind.BcEncrypt);
            var nonceReply = BcMessage.FromXml(new BcMeta { MsgId = 1, MsgNum = upgrade.Meta.MsgNum,
                Class = BcConstants.ClassModernNoOffset, ResponseCode = 0xdd12 },
                BcXmlBody.FromRaw(new XElement("Encryption", new XElement("type", "fullaes"), new XElement("nonce", nonce))));
            await stream.WriteAsync(BcCodec.Serialize(nonceReply, context.Encryption), stop);
            var login = await BcCodec.ReadMessageAsync(stream, context, stop);
            Check(login.Meta.MsgId == 1 && login.Xml?.LoginUser != null, "fixture did not establish modern login");
            var success = BcMessage.FromXml(new BcMeta { MsgId = 1, MsgNum = login.Meta.MsgNum,
                Class = BcConstants.ClassModernNoOffset, ResponseCode = 200 }, BcXmlBody.FromRaw(new XElement("LoginSuccess", "ok")));
            await stream.WriteAsync(BcCodec.Serialize(success, context.Encryption), stop);
            context.Encryption.Set(EncryptionKind.FullAes, Md5Utils.MakeAesKey(nonce, password));
            var request = await BcCodec.ReadMessageAsync(stream, context, stop); AssertSnap(request);
            await Reply(stream, request, size: jpeg.Length, ct: stop, encryption: context.Encryption);
            await Reply(stream, request, bytes: jpeg[..37], ct: stop, encryption: context.Encryption);
            await Reply(stream, request, bytes: jpeg[37..], ct: stop, encryption: context.Encryption, responseCode: 201);
        }, async (camera, stop) =>
        {
            await camera.LoginAsync("synthetic-snapshot-user", password, stop);
            Check((await camera.SnapBoundedAsync(1024, stop))!.SequenceEqual(jpeg),
                "FullAES snapshot chunks changed or required another login");
        });
    }

    private static void AssertSnap(BcMessage request, string streamType = "sub")
    {
        Check(request.Meta.MsgId == BcConstants.MsgIdSnap && request.Meta.MsgId == 109,
            "native snapshot performed a login or started a stream");
        Check(request.Xml?.RawElement("Snap")?.Element("streamType")?.Value == streamType,
            "native snapshot does not request the camera's small tier");
    }

    private static async Task Reply(NetworkStream stream, BcMessage request, int? size = null,
        byte[]? bytes = null, CancellationToken ct = default, EncryptionState? encryption = null,
        ushort responseCode = 200)
    {
        encryption ??= new EncryptionState();
        bool encrypted = bytes != null && encryption.Snapshot().Item1 == EncryptionKind.FullAes;
        var response = new BcMessage
        {
            Meta = new BcMeta { MsgId = 109, MsgNum = request.Meta.MsgNum, ChannelId = request.Meta.ChannelId,
                Class = BcConstants.ClassModern, ResponseCode = responseCode },
            Xml = size.HasValue ? BcXmlBody.FromRaw(new XElement("Snap", new XElement("pictureSize", size.Value))) : null,
            Extension = bytes != null ? new ExtensionXml { BinaryData = 1, ChannelId = request.Meta.ChannelId,
                EncryptLen = encrypted ? (uint)bytes.Length : null } : null,
            Binary = encrypted ? XmlCrypto.Encrypt(request.Meta.ChannelId, bytes!, encryption) : bytes
        };
        if (encrypted)
        {
            // ExtensionXml.Serialize models outgoing controls and omits the
            // inbound-only encryptLen. Build that real camera reply explicitly.
            var extension = new XElement("Extension", new XAttribute("version", "1.1"),
                new XElement("binaryData", 1), new XElement("channelId", request.Meta.ChannelId),
                new XElement("encryptLen", bytes!.Length));
            byte[] encryptedExtension = XmlCrypto.Encrypt(request.Meta.ChannelId,
                Encoding.UTF8.GetBytes(extension.ToString(SaveOptions.DisableFormatting)), encryption);
            response.Extension = null;
            response.Binary = encryptedExtension.Concat(response.Binary!).ToArray();
            byte[] packet = BcCodec.Serialize(response, encryption);
            BinaryPrimitives.WriteUInt32LittleEndian(packet.AsSpan(20, 4), (uint)encryptedExtension.Length);
            await stream.WriteAsync(packet, ct);
        }
        else await stream.WriteAsync(BcCodec.Serialize(response, encryption), ct);
    }

    private static async Task Wire(Func<NetworkStream, BcContext, CancellationToken, Task> server,
        Func<IBcCamera, CancellationToken, Task> run)
    {
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
        var serving = Task.Run(async () =>
        {
            using var accepted = await listener.AcceptTcpClientAsync(stop.Token);
            await server(accepted.GetStream(), new BcContext(new EncryptionState()), stop.Token);
        });
        try
        {
            await using var camera = await BcCamera.ConnectAsync("127.0.0.1", ((IPEndPoint)listener.LocalEndpoint).Port, 0, stop.Token);
            await run(camera, stop.Token); await serving;
        }
        finally { stop.Cancel(); listener.Stop(); try { await serving; } catch (OperationCanceledException) { } }
    }

    private sealed class Camera(Func<CancellationToken, Task<byte[]?>> capture) : IBcCamera
    {
        private int _calls;
        public int Calls => Volatile.Read(ref _calls);
        public Func<CancellationToken, Task<byte[]?>> Capture { get; set; } = capture;
        public byte ChannelId => 0;
        public DeviceInfoXml? DeviceInfo => null;
        public IPAddress? RemoteIp => IPAddress.Loopback;
        public Task<byte[]?> SnapAsync(CancellationToken ct) { Interlocked.Increment(ref _calls); return Capture(ct); }
        public Task LoginAsync(string username, string? password, CancellationToken ct, BcLoginMode? mode = null) => throw new Exception("snapshot attempted a new login");
        public Task StartVideoAsync(StreamKind stream, ChannelWriter<byte[]> output, Func<bool>? tolerable, CancellationToken ct) => throw new Exception("snapshot attempted a new source stream");
        public Task PingAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task WatchMotionAsync(Action<MotionPush> callback, CancellationToken ct) => throw new NotSupportedException();
        public Task WatchStatusAsync(Action<StatusPush> callback, CancellationToken ct) => throw new NotSupportedException();
        public Task TalkAsync(TalkAbilityXml ability, ChannelReader<byte[]> frames, CancellationToken ct) => throw new NotSupportedException();
        public Task<BcMessage?> SendCommandAsync(uint id, BcXmlBody? xml = null, ExtensionXml? extension = null,
            TimeSpan? timeout = null, bool tolerateNoReply = false, CancellationToken ct = default) => throw new NotSupportedException();
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
