using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using System.Xml.Linq;
using Neolink.Bc;
using Neolink.Bc.Xml;
using Neolink.Config;
using Neolink.Media;
using Neolink.Protocol;
using Neolink.Streaming;

internal static class VideoProgressDeadlineTests
{
    public static async Task OnlyVideoRenewsDeadline()
    {
        using var preview = new CancellationTokenSource();
        await using var guard = new VideoProgressDeadline(preview, timeout: TimeSpan.FromMilliseconds(300));
        var clock = Stopwatch.StartNew();
        while (!preview.IsCancellationRequested && clock.Elapsed < TimeSpan.FromSeconds(2))
        {
            guard.Observe(new MediaInfo(640, 480, 20));
            guard.Observe(new AacFrame(new byte[] { 0xff, 0xf1 }));
            await Task.Delay(20);
        }
        Check(preview.IsCancellationRequested && guard.Expired, "metadata or audio renewed the video deadline");

        using var healthy = new CancellationTokenSource();
        await using var healthyGuard = new VideoProgressDeadline(healthy, timeout: TimeSpan.FromMilliseconds(300));
        for (int i = 0; i < 30; i++)
        {
            healthyGuard.Observe(new VideoFrame(VideoCodec.H264, true, 0, null, new byte[] { 1 }));
            await Task.Delay(20);
        }
        Check(!healthy.IsCancellationRequested && !healthyGuard.Expired, "actual video failed to renew its deadline");
    }

    public static async Task PrivacyAndPartialReadCancellation()
    {
        bool privacy = true;
        using var preview = new CancellationTokenSource();
        await using (var guard = new VideoProgressDeadline(preview, () => privacy, TimeSpan.FromMilliseconds(200)))
        {
            await Task.Delay(600);
            Check(!preview.IsCancellationRequested, "privacy was treated as a video outage");
            privacy = false;
            await Until(() => preview.IsCancellationRequested, TimeSpan.FromSeconds(2));
            Check(guard.Expired, "leaving privacy never armed a first-video deadline");
        }

        using var partial = new CancellationTokenSource();
        var chunks = Channel.CreateUnbounded<byte[]>();
        // A genuine video header starts but never supplies the rest of the frame.
        await chunks.Writer.WriteAsync(new byte[] { 0x30, 0x30, 0x64, 0x63 });
        var reader = new MediaFrameReader(chunks.Reader);
        await using var partialGuard = new VideoProgressDeadline(partial, timeout: TimeSpan.FromMilliseconds(200));
        try { await reader.ReadFrameAsync(partial.Token).AsTask().WaitAsync(TimeSpan.FromSeconds(2)); }
        catch (OperationCanceledException) when (partialGuard.Expired) { return; }
        throw new Exception("a partial frame outlived its video deadline");
    }

    public static Task MainMetadataStallReconnects() => WireRecovery(mainStall: true);
    public static Task LowMetadataStallOnlyRetriesPreview() => WireRecovery(mainStall: false);

    public static async Task ProductionDeadlineWireRecovery()
    {
        await WireRecovery(mainStall: true, productionDeadline: true);
        await WireRecovery(mainStall: false, productionDeadline: true);
    }

    private static async Task WireRecovery(bool mainStall, bool productionDeadline = false)
    {
        await using var peer = new ProgressPeer(mainStall ? 1 : 2);
        var main = new StreamHub("synthetic-progress-main"); var low = new StreamHub("synthetic-progress-low");
        var source = NewSource(peer.Port, main, low,
            timeout: productionDeadline ? VideoProgressDeadline.DefaultTimeout : TimeSpan.FromMilliseconds(800));
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(40));
        var running = source.RunAsync(stop.Token);
        try
        {
            await Until(() => mainStall ? peer.Logins >= 2 && main.LiveVideo && low.LiveVideo
                : peer.LowRequests >= 2 && main.LiveVideo && low.LiveVideo, TimeSpan.FromSeconds(productionDeadline ? 35 : 12));
            Check(main.GetVideoDiagnostics().LastVideoAgeMs < 1000 && low.GetVideoDiagnostics().LastVideoAgeMs < 1000,
                "recovered previews did not deliver new pictures");
            if (mainStall)
                Check(peer.Logins == 2 && peer.MainRequests == 2 && main.SourceEpoch == 1,
                    "main video stall did not use exactly one normal session reconnect");
            else
                Check(peer.Logins == 1 && peer.Connections == 1 && peer.MainRequests == 1
                    && peer.LowRequests == 2 && peer.LowStops >= 1 && main.SourceEpoch == 0 && low.SourceEpoch == 1,
                    "low stall disturbed the main preview/login or failed to release its own handle");
        }
        finally
        {
            stop.Cancel();
            await running.WaitAsync(TimeSpan.FromSeconds(3));
        }
    }

    public static async Task PrivacySourceKeepsMetadataSession()
    {
      foreach (bool relay in new[] { true, false })
      {
        await using var peer = new ProgressPeer(3);
        var main = new StreamHub("synthetic-privacy-main");
        var low = relay ? new StreamHub("synthetic-privacy-low") : null;
        var source = NewSource(peer.Port, main, low, relay: relay);
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var running = source.RunAsync(stop.Token);
        try
        {
            await Until(() => source.PrivacyOn == true && peer.MainRequests == 1 && (!relay || peer.LowRequests == 1), TimeSpan.FromSeconds(5));
            await Task.Delay(1600);
            Check(source.PrivacyOn == true && peer.Logins == 1 && peer.MainRequests == 1 && !running.IsCompleted,
                "metadata cleared privacy or caused a reconnect while deliberately dark");
            peer.Privacy = false;
            await Until(() => main.LiveVideo && (low == null || low.LiveVideo) && source.PrivacyOn == false, TimeSpan.FromSeconds(2));
            Check(peer.Logins == 1 && peer.MainRequests == 1 && (!relay || peer.LowRequests == 1), "privacy exit needed a new session or preview");
        }
        finally { stop.Cancel(); await running.WaitAsync(TimeSpan.FromSeconds(3)); }
      }
    }

    public static async Task IdleDemandAndSuspensionDoNotArmPreview()
    {
        foreach (bool sleep in new[] { false, true })
        {
            await using var peer = new ProgressPeer(0);
            var hub = new StreamHub("synthetic-idle");
            var config = Config(peer.Port, sleep ? false : null);
            // Explicit sleep policy parks without connecting; an idle mains
            // source may log in for controls but must not request video.
            var source = new CameraService(config, StreamKind.Main, hub, TimeSpan.Zero)
                { RelayOnly = true, MediaOnDemand = true, VideoProgressTimeout = TimeSpan.FromMilliseconds(200) };
            if (sleep) source.BatteryDetected();
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(4));
            var running = source.RunAsync(stop.Token);
            await Task.Delay(700);
            Check(peer.MainRequests == 0 && (sleep ? peer.Logins == 0 : peer.Logins == 1),
                "idle/sleep policy started or restarted an unwanted preview");
            stop.Cancel(); await running.WaitAsync(TimeSpan.FromSeconds(3));
        }
        await using var suspendedPeer = new ProgressPeer(0);
        var suspended = NewSource(suspendedPeer.Port, new StreamHub("synthetic-suspended"), null);
        suspended.SetSuspended(true);
        using var suspendedStop = new CancellationTokenSource();
        var parked = suspended.RunAsync(suspendedStop.Token);
        await Task.Delay(500);
        Check(suspendedPeer.Connections == 0, "suspended source opened a connection");
        suspendedStop.Cancel(); await parked.WaitAsync(TimeSpan.FromSeconds(2));
    }

    private static CameraConfig Config(int port, bool? alwaysOn = true) => new()
    {
        Name="synthetic-video-progress", Host="127.0.0.1", Port=port,
        Username="synthetic-progress-user", Password="synthetic-progress-password",
        Stream="mainStream", AlwaysOn=alwaysOn, Record=false, Udp=false, UdpProbe=false
    };
    private static CameraService NewSource(int port, StreamHub main, StreamHub? low, bool relay = true, TimeSpan? timeout = null)
        => new(Config(port), StreamKind.Main, main, TimeSpan.Zero)
        { RelayOnly=relay, SecondaryVideoSink=low, VideoProgressTimeout=timeout ?? TimeSpan.FromMilliseconds(800) };
    private static void Check(bool value, string message) { if (!value) throw new Exception(message); }
    private static async Task Until(Func<bool> ready, TimeSpan timeout)
    {
        var clock=Stopwatch.StartNew();
        while (!ready()) { Check(clock.Elapsed<timeout,"timed out awaiting video-progress recovery"); await Task.Delay(10); }
    }

    private sealed class ProgressPeer : IAsyncDisposable
    {
        private readonly TcpListener _listener=new(IPAddress.Loopback,0);
        private readonly CancellationTokenSource _stop=new();
        private readonly SemaphoreSlim _writes=new(1);
        private readonly int _mode;
        private readonly Task _serve;
        private int _connections,_logins,_mainRequests,_lowRequests,_lowStops;
        private int _privacy;
        public int Port { get; }
        public int Connections=>Volatile.Read(ref _connections);
        public int Logins=>Volatile.Read(ref _logins);
        public int MainRequests=>Volatile.Read(ref _mainRequests);
        public int LowRequests=>Volatile.Read(ref _lowRequests);
        public int LowStops=>Volatile.Read(ref _lowStops);
        public bool Privacy { get=>Volatile.Read(ref _privacy)!=0; set=>Volatile.Write(ref _privacy,value?1:0); }
        public ProgressPeer(int mode)
        {
            _mode=mode; Privacy=mode==3;
            _listener.Start();Port=((IPEndPoint)_listener.LocalEndpoint).Port;_serve=Serve();
        }
        private async Task Write(NetworkStream stream,BcMessage message,CancellationToken ct)
        {
            await _writes.WaitAsync(ct);
            try { await stream.WriteAsync(BcCodec.Serialize(message,new EncryptionState()),ct); }
            finally { _writes.Release(); }
        }
        private static BcMeta Reply(BcMessage request)=>new()
            {MsgId=request.Meta.MsgId,MsgNum=request.Meta.MsgNum,ChannelId=request.Meta.ChannelId,
             StreamType=0,Class=BcConstants.ClassModern,ResponseCode=200};
        private async Task Serve()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client=await _listener.AcceptTcpClientAsync(_stop.Token);
                    Interlocked.Increment(ref _connections);
                    using var session=CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
                    var stream=client.GetStream();var context=new BcContext(new EncryptionState());
                    var previews=new ConcurrentDictionary<ushort,(bool Main,int Attempt,long Began)>();
                    Task? media=null;
                    try
                    {
                        var upgrade=await BcCodec.ReadMessageAsync(stream,context,session.Token);
                        await Write(stream,BcMessage.FromXml(new BcMeta{MsgId=1,MsgNum=upgrade.Meta.MsgNum,
                          Class=BcConstants.ClassModernNoOffset,ResponseCode=0xdd00},
                          BcXmlBody.FromRaw(new XElement("Encryption",new XElement("type","none"),new XElement("nonce","SYNTHETIC-PROGRESS")))),session.Token);
                        var login=await BcCodec.ReadMessageAsync(stream,context,session.Token);
                        Check(login.Xml?.LoginUser!=null,"expected synthetic login");Interlocked.Increment(ref _logins);
                        await Write(stream,BcMessage.FromXml(Reply(login),BcXmlBody.FromRaw(new XElement("SyntheticLogin","ok"))),session.Token);
                        media=Pump(stream,previews,session.Token);
                        while (!session.IsCancellationRequested)
                        {
                            var request=await BcCodec.ReadMessageAsync(stream,context,session.Token);
                            if (request.Meta.MsgId==3)
                            {
                                bool main=request.Xml?.Preview?.Handle==0;
                                int attempt=main?Interlocked.Increment(ref _mainRequests):Interlocked.Increment(ref _lowRequests);
                                await Write(stream,BcMessage.HeaderOnly(Reply(request)),session.Token);
                                await Write(stream,Media(request.Meta.MsgNum,Info(main)),session.Token);
                                previews[request.Meta.MsgNum]=(main,attempt,Stopwatch.GetTimestamp());
                            }
                            else if (request.Meta.MsgId==4)
                            {
                                Check(request.Xml?.Preview?.Handle==256,"low retry stopped wrong preview");
                                Interlocked.Increment(ref _lowStops);previews.TryRemove(request.Meta.MsgNum,out _);
                                await Write(stream,BcMessage.HeaderOnly(Reply(request)),session.Token);
                            }
                            else await Write(stream,BcMessage.HeaderOnly(Reply(request)),session.Token);
                        }
                    }
                    catch (EndOfStreamException) { }
                    catch (IOException) { }
                    catch (OperationCanceledException) when (session.IsCancellationRequested) { }
                    finally
                    {
                        session.Cancel();if(media!=null)try{await media;}catch(OperationCanceledException){}catch(IOException){}
                    }
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        }
        private async Task Pump(NetworkStream stream,ConcurrentDictionary<ushort,(bool Main,int Attempt,long Began)> previews,CancellationToken ct)
        {
            using var timer=new PeriodicTimer(TimeSpan.FromMilliseconds(40));int ticks=0;
            while(await timer.WaitForNextTickAsync(ct))
            {
                if(_mode==3 && ++ticks%2==0)
                    await Write(stream,BcMessage.FromXml(new BcMeta{MsgId=623,MsgNum=0,Class=BcConstants.ClassModern,ResponseCode=200},
                      BcXmlBody.FromRaw(new XElement("sleepStatus",new XElement("status",Privacy?1:0)))),ct);
                foreach(var(number,p)in previews)
                {
                    bool stalled=(_mode==3&&Privacy)||p.Attempt==1&&Stopwatch.GetElapsedTime(p.Began)>TimeSpan.FromMilliseconds(150)
                        &&(_mode==1&&p.Main||_mode==2&&!p.Main);
                    await Write(stream,Media(number,stalled?Info(p.Main):Video(p.Main)),ct);
                    if(stalled)await Write(stream,Media(number,Audio()),ct);
                }
            }
        }
        private static BcMessage Media(ushort number,byte[] data)=>new()
        {Meta=new BcMeta{MsgId=3,MsgNum=number,StreamType=0,Class=BcConstants.ClassModern,ResponseCode=200},
         Extension=new ExtensionXml{BinaryData=1,ChannelId=0},Binary=data};
        private static byte[] Info(bool main)
        {
            var b=new byte[32];BinaryPrimitives.WriteUInt32LittleEndian(b,0x31303031);
            BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(4),32);BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(8),main?4512u:896u);
            BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(12),main?2512u:512u);b[17]=20;return b;
        }
        private static byte[] Video(bool main)
        {
            var b=new byte[32];BinaryPrimitives.WriteUInt32LittleEndian(b,0x63643030);
            Encoding.ASCII.GetBytes(main?"H265":"H264",b.AsSpan(4));BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(8),5);
            b[27]=1;b[28]=main?(byte)0x26:(byte)0x64;return b;
        }
        private static byte[] Audio()
        {
            var b=new byte[16];BinaryPrimitives.WriteUInt32LittleEndian(b,0x62773530);
            BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(4),2);b[8]=0xff;b[9]=0xf1;return b;
        }
        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();_listener.Stop();await _serve.WaitAsync(TimeSpan.FromSeconds(3));
            _stop.Dispose();_writes.Dispose();
        }
    }
}
