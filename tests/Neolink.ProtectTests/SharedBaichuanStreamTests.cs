using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Xml.Linq;
using Neolink.Bc;
using Neolink.Bc.Xml;
using Neolink.Config;
using Neolink.Media;
using Neolink.Protocol;
using Neolink.Streaming;

internal static class SharedBaichuanStreamTests
{
    public static async Task MessageNumberRouting()
    {
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
        try
        {
            Task<TcpClient> accept = listener.AcceptTcpClientAsync(stop.Token).AsTask();
            await using var connection = await BcConnection.ConnectAsync("127.0.0.1",
                ((IPEndPoint)listener.LocalEndpoint).Port, TimeSpan.FromSeconds(2), stop.Token);
            using var peer = await accept;
            using var main = connection.Subscribe(3, 2);
            using var low = connection.Subscribe(3, 3);
            using var wildcard = connection.Subscribe(3);
            using var motion = connection.Subscribe(33);
            bool duplicate = false;
            try { using var invalid = connection.Subscribe(3, 2); }
            catch (InvalidOperationException) { duplicate = true; }
            Check(duplicate, "duplicate exact video subscription is accepted");
            foreach (var (id, number) in new (uint, ushort)[] { (3, 3), (33, 91), (3, 2), (3, 77) })
                await Send(peer.GetStream(), BcMessage.HeaderOnly(new BcMeta
                    { MsgId = id, MsgNum = number, Class = BcConstants.ClassModern, ResponseCode = 200 }), stop.Token);
            Check((await main.ReceiveAsync(TimeSpan.FromSeconds(1), stop.Token)).Meta.MsgNum == 2
                && (await low.ReceiveAsync(TimeSpan.FromSeconds(1), stop.Token)).Meta.MsgNum == 3,
                "interleaved qualities ignore their request numbers");
            Check((await motion.ReceiveAsync(TimeSpan.FromSeconds(1), stop.Token)).Meta.MsgNum == 91,
                "exact video subscriptions swallowed an unsolicited motion push");
            Check((await wildcard.ReceiveAsync(TimeSpan.FromSeconds(1), stop.Token)).Meta.MsgNum == 77
                && !wildcard.Messages.TryRead(out _), "exact routing duplicates messages into the legacy watch");
            main.Dispose();
            await Send(peer.GetStream(), BcMessage.HeaderOnly(new BcMeta
                { MsgId = 3, MsgNum = 2, Class = BcConstants.ClassModern, ResponseCode = 200 }), stop.Token);
            Check((await wildcard.ReceiveAsync(TimeSpan.FromSeconds(1), stop.Token)).Meta.MsgNum == 2,
                "disposing an exact subscription removes the legacy watch");
        }
        finally { listener.Stop(); }
    }

    public static async Task StopAcknowledgementReleasesBinaryMode()
    {
        var context = new BcContext(new EncryptionState());
        context.InBinMode.Add(42); context.InBinMode.Add(43);
        var acknowledgement = BcMessage.FromXml(new BcMeta
            { MsgId = 4, MsgNum = 42, Class = BcConstants.ClassModern, ResponseCode = 200 },
            new BcXmlBody { Preview = new PreviewXml { ChannelId = 0, Handle = 256, StreamType = null } });
        Check(!Encoding.UTF8.GetString(acknowledgement.Xml!.Serialize()).Contains("streamType", StringComparison.Ordinal),
            "stop preview serializes a start-only streamType element");
        using var wire = new MemoryStream(BcCodec.Serialize(acknowledgement, context.Encryption));
        var reply = await BcCodec.ReadMessageAsync(wire, context, CancellationToken.None);
        Check(reply.Xml?.Preview?.Handle == 256 && reply.Binary == null
            && !context.InBinMode.Contains(42) && context.InBinMode.Contains(43),
            "stop acknowledgement retains stale binary mode or resets the main decoder");
        context.InBinMode.Add(42);
        using var refused = new MemoryStream(BcCodec.Serialize(BcMessage.HeaderOnly(new BcMeta
            { MsgId = 4, MsgNum = 42, Class = BcConstants.ClassModern, ResponseCode = 500 }), context.Encryption));
        await BcCodec.ReadMessageAsync(refused, context, CancellationToken.None);
        Check(context.InBinMode.Contains(42), "refused stop prematurely resets preview framing");
    }

    public static async Task OneLoginDemuxAndLowRetry()
    {
        await using var peer = new SharedPeer();
        var main = new ProbeSink(); var low = new ProbeSink();
        var source = new CameraService(new CameraConfig
        {
            Name = "synthetic-shared", Host = "127.0.0.1", Port = peer.Port,
            Username = "synthetic-shared-user", Password = "synthetic-shared-password", Stream = "mainStream",
            AlwaysOn = true, Udp = false, UdpProbe = false, Record = false
        }, StreamKind.Main, main, TimeSpan.Zero)
            { RelayOnly = true, SecondaryVideoSink = low, MotionSink = _ => { } };
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        Task running = source.RunAsync(stop.Token);
        try
        {
            await Until(() => peer.LowRequests == 1 && low.Stops >= 1, TimeSpan.FromSeconds(4));
            int before = main.Frames;
            await Task.Delay(250, stop.Token);
            Check(main.Frames > before && main.Stops == 0 && source.LiveCamera != null && !running.IsCompleted,
                "a low preview rejection resets/stops the main session");
            await Until(() => low.Frames >= 3, TimeSpan.FromSeconds(7));
            Check(peer.Connections == 1 && peer.Logins == 1 && peer.MainRequests == 1
                && peer.LowRequests == 2 && peer.MotionSubscriptions == 1,
                "low retry opens another login, main preview or event subscription");
            Check(main.Width == 4512 && main.Codec == VideoCodec.H265 && main.Marker == 0x26
                && low.Width == 896 && low.Codec == VideoCodec.H264 && low.Marker == 0x64,
                "StreamType=0 responses mix main and low decoders");
            Check(peer.LowNumbers.Distinct().Count() == 2 && peer.LowStops >= 1,
                "low retry reuses an old request number or fails to release handle256");
            Check(main.Stops == 0 && low.Stops == 1 && peer.MainStops == 0,
                "independent retry flushes/stops the main preview");
        }
        finally
        {
            var elapsed = Stopwatch.StartNew(); stop.Cancel();
            await running.WaitAsync(TimeSpan.FromSeconds(3));
            Check(elapsed.Elapsed < TimeSpan.FromSeconds(3) && main.Stops == 1 && low.Stops == 2,
                "shared-session cancellation strands a preview or stale low frames");
        }
    }

    private static async Task Until(Func<bool> ready, TimeSpan timeout)
    {
        var elapsed = Stopwatch.StartNew();
        while (!ready())
        {
            Check(elapsed.Elapsed < timeout, "timed out awaiting shared preview state");
            await Task.Delay(10);
        }
    }
    private static void Check(bool result, string message) { if (!result) throw new Exception(message); }
    private static async Task Send(NetworkStream stream, BcMessage message, CancellationToken ct)
        => await stream.WriteAsync(BcCodec.Serialize(message, new EncryptionState()), ct);

    private sealed class ProbeSink : IMediaSink
    {
        private int _frames, _stops;
        public int Frames => Volatile.Read(ref _frames);
        public int Stops => Volatile.Read(ref _stops);
        public uint Width { get; private set; }
        public VideoCodec Codec { get; private set; }
        public byte Marker { get; private set; }
        public void PublishInfo(MediaInfo info) => Width = info.Width;
        public void PublishVideo(VideoFrame frame)
        {
            Codec = frame.Codec; Marker = frame.Data[^1]; Interlocked.Increment(ref _frames);
        }
        public void PublishAac(AacFrame frame) { }
        public void PublishAdpcm(AdpcmFrame frame) { }
        public void SourceStopped() => Interlocked.Increment(ref _stops);
        public void SourceAuthenticationFailed() => throw new Exception("synthetic shared login unexpectedly refused");
    }

    private sealed class SharedPeer : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly SemaphoreSlim _writes = new(1);
        private readonly ConcurrentDictionary<ushort, bool> _previews = new();
        private readonly Task _serving;
        private int _connections, _logins, _mainRequests, _lowRequests, _lowStops, _mainStops, _motionSubscriptions;
        public int Port { get; }
        public int Connections => Volatile.Read(ref _connections);
        public int Logins => Volatile.Read(ref _logins);
        public int MainRequests => Volatile.Read(ref _mainRequests);
        public int LowRequests => Volatile.Read(ref _lowRequests);
        public int LowStops => Volatile.Read(ref _lowStops);
        public int MainStops => Volatile.Read(ref _mainStops);
        public int MotionSubscriptions => Volatile.Read(ref _motionSubscriptions);
        public ConcurrentBag<ushort> LowNumbers { get; } = new();
        public SharedPeer()
        {
            _listener.Start(); Port = ((IPEndPoint)_listener.LocalEndpoint).Port; _serving = Serve();
        }
        private async Task Write(NetworkStream stream, BcMessage response)
        {
            await _writes.WaitAsync(_stop.Token);
            try { await Send(stream, response, _stop.Token); }
            finally { _writes.Release(); }
        }
        private static BcMeta ReplyMeta(BcMessage request, ushort code = 200) => new()
            { MsgId = request.Meta.MsgId, MsgNum = request.Meta.MsgNum, ChannelId = request.Meta.ChannelId,
                StreamType = 0, Class = BcConstants.ClassModern, ResponseCode = code };
        private async Task Serve()
        {
            try
            {
                using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                Interlocked.Increment(ref _connections);
                var stream = client.GetStream(); var context = new BcContext(new EncryptionState());
                var upgrade = await BcCodec.ReadMessageAsync(stream, context, _stop.Token);
                var nonceMeta = new BcMeta { MsgId = BcConstants.MsgIdLogin, MsgNum = upgrade.Meta.MsgNum,
                    Class = BcConstants.ClassModernNoOffset, ResponseCode = 0xdd00 };
                await Write(stream, BcMessage.FromXml(nonceMeta, BcXmlBody.FromRaw(new XElement("Encryption",
                    new XElement("type", "none"), new XElement("nonce", "SYNTHETIC-SHARED-NONCE")))));
                var login = await BcCodec.ReadMessageAsync(stream, context, _stop.Token);
                Check(login.Xml?.LoginUser != null, "shared synthetic peer expected exactly one login handshake");
                Interlocked.Increment(ref _logins);
                await Write(stream, BcMessage.FromXml(ReplyMeta(login), BcXmlBody.FromRaw(new XElement("SyntheticLogin", "ok"))));
                Task media = PumpMedia(stream);
                try
                {
                    while (!_stop.IsCancellationRequested)
                    {
                        var request = await BcCodec.ReadMessageAsync(stream, context, _stop.Token);
                        switch (request.Meta.MsgId)
                        {
                            case 3:
                                bool main = request.Xml?.Preview?.Handle == 0;
                                if (main) Interlocked.Increment(ref _mainRequests);
                                else
                                {
                                    Check(request.Xml?.Preview?.Handle == 256 && request.Xml.Preview.StreamType == "subStream",
                                        "shared low preview requests incorrect native handle");
                                    LowNumbers.Add(request.Meta.MsgNum);
                                    if (Interlocked.Increment(ref _lowRequests) == 1)
                                    {
                                        await Write(stream, BcMessage.FromXml(ReplyMeta(request, 500),
                                            BcXmlBody.FromRaw(new XElement("PreviewError", "synthetic-private-body"))));
                                        break;
                                    }
                                }
                                await Write(stream, BcMessage.HeaderOnly(ReplyMeta(request)));
                                await Write(stream, MediaMessage(request.Meta.MsgNum, MediaInfoBytes(main)));
                                _previews[request.Meta.MsgNum] = main;
                                break;
                            case 4:
                                if (request.Xml?.Preview?.Handle == 256)
                                {
                                    Check(string.IsNullOrEmpty(request.Xml.Preview.StreamType), "stop includes start-only stream type");
                                    Interlocked.Increment(ref _lowStops);
                                }
                                else Interlocked.Increment(ref _mainStops);
                                _previews.TryRemove(request.Meta.MsgNum, out _);
                                await Write(stream, BcMessage.HeaderOnly(ReplyMeta(request)));
                                break;
                            case 31:
                                Interlocked.Increment(ref _motionSubscriptions);
                                await Write(stream, BcMessage.HeaderOnly(ReplyMeta(request)));
                                break;
                            default: throw new Exception("shared relay issued an unrelated command");
                        }
                    }
                }
                finally
                {
                    _stop.Cancel(); try { await media; } catch (OperationCanceledException) { }
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
            catch (EndOfStreamException) { }
            catch (IOException) when (_stop.IsCancellationRequested) { }
        }
        private async Task PumpMedia(NetworkStream stream)
        {
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(40));
            while (await timer.WaitForNextTickAsync(_stop.Token))
                foreach (var (number, main) in _previews)
                    await Write(stream, MediaMessage(number, VideoBytes(main)));
        }
        private static BcMessage MediaMessage(ushort number, byte[] payload) => new()
        {
            Meta = new BcMeta { MsgId = 3, MsgNum = number, StreamType = 0,
                Class = BcConstants.ClassModern, ResponseCode = 200 },
            Extension = new ExtensionXml { BinaryData = 1, ChannelId = 0 }, Binary = payload
        };
        private static byte[] MediaInfoBytes(bool main)
        {
            var bytes = new byte[32]; BinaryPrimitives.WriteUInt32LittleEndian(bytes, 0x31303031);
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(4), 32);
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(8), main ? 4512u : 896u);
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(12), main ? 2512u : 512u); bytes[17] = 20;
            return bytes;
        }
        private static byte[] VideoBytes(bool main)
        {
            var bytes = new byte[32]; BinaryPrimitives.WriteUInt32LittleEndian(bytes, 0x63643030);
            Encoding.ASCII.GetBytes(main ? "H265" : "H264", bytes.AsSpan(4));
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(8), 5);
            bytes[24] = 0; bytes[25] = 0; bytes[26] = 0; bytes[27] = 1; bytes[28] = main ? (byte)0x26 : (byte)0x64;
            return bytes;
        }
        public async ValueTask DisposeAsync()
        {
            _stop.Cancel(); _listener.Stop();
            try { await _serving.WaitAsync(TimeSpan.FromSeconds(3)); }
            finally { _stop.Dispose(); _writes.Dispose(); }
        }
    }
}
