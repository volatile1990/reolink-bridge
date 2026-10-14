using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text.Json;
using System.Threading.Channels;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class TalkRecoveryTests
{
    private static void Check(bool value, string message) { if (!value) throw new Exception(message); }
    private static async Task Until(Func<bool> ready)
    {
        var elapsed = Stopwatch.StartNew();
        while (!ready())
        {
            Check(elapsed.Elapsed < TimeSpan.FromSeconds(3), "talk recovery did not progress");
            await Task.Delay(10);
        }
    }

    public static async Task FreshSpeechAfterTransientFailure()
    {
        foreach (int failure in new[] { 0, 1, 2 })
        {
            var control = DispatchProxy.Create<ICameraControl, RecoveryControl>();
            var probe = (RecoveryControl)(object)control; probe.Failure = failure;
            var receiver = new BackchannelReceiver(control, TimeSpan.FromMilliseconds(150));
            receiver.Start(CancellationToken.None);
            receiver.OnPcm([1, 0, 2, 0]);
            await Until(() => Volatile.Read(ref probe.Attempts) == 1);
            await Task.Delay(30);
            receiver.OnPcm([3, 0, 4, 0]); // stale speech received during recovery is discarded
            await Task.Delay(30);
            Check(probe.Attempts == 1, "talk recovery ignored its backoff");
            await Task.Delay(160);
            byte[] fresh = [5, 0, 6, 0];
            receiver.OnPcm(fresh);
            byte[] delivered = await probe.Pcm.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));
            Check(probe.Attempts == 2 && delivered.SequenceEqual(fresh) && !probe.Pcm.Reader.TryRead(out _),
                "recovered talk replayed speech from its failed attempt");
            await receiver.StopAsync().WaitAsync(TimeSpan.FromSeconds(2));
            Check(probe.Completed == 1, "talk shutdown did not await its successful attempt");
            receiver.OnPcm([7, 0]);
            await Task.Delay(20);
            Check(probe.Attempts == 2, "stopped receiver accepted speech");
        }
    }

    public static async Task NativeDecoderFailureRecovery()
    {
        byte[] aac = await NativeTalkUdpTests.MakeAac();
        var blocks = new List<byte[]>();
        for (int offset = 0; offset < aac.Length;)
        {
            int length = ((aac[offset + 3] & 3) << 11) | (aac[offset + 4] << 3) | (aac[offset + 5] >> 5);
            blocks.Add(aac.AsSpan(offset, length).ToArray()); offset += length;
        }
        var control = DispatchProxy.Create<ICameraControl, RecoveryControl>();
        var probe = (RecoveryControl)(object)control; probe.Failure = -1;
        int port;
        using (var reservation = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0)))
            port = ((IPEndPoint)reservation.Client.LocalEndPoint!).Port;
        int decoders = 0;
        var sink = new NativeTalkUdpSink(control, IPAddress.Loopback, port, [IPAddress.Loopback])
        {
            DecoderRecoveryDelay = TimeSpan.FromMilliseconds(100),
            DecoderFactory = (output, error, token) => Interlocked.Increment(ref decoders) == 1
                ? new NativeAacTalkDecoder(output, error, token, _ => null)
                : new NativeAacTalkDecoder(output, error, token)
        };
        using var stop = new CancellationTokenSource();
        Task running = sink.RunAsync(stop.Token);
        JsonElement Diagnostics() => JsonSerializer.SerializeToElement(sink.Diagnostics());
        try
        {
            sink.Accept(blocks[0], IPAddress.Loopback, stop.Token);
            await Until(() => Diagnostics().GetProperty("decoderErrors").GetInt64() == 1);
            sink.Accept(blocks[1], IPAddress.Loopback, stop.Token);
            Check(decoders == 1 && probe.Attempts == 0, "failed AAC decoder replayed speech or restarted without backoff");
            await Task.Delay(140);
            foreach (byte[] block in blocks)
            {
                sink.Accept(block, IPAddress.Loopback, stop.Token);
                await Task.Delay(15);
            }
            byte[] pcm = await probe.Pcm.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(3));
            Check(pcm.Length > 0 && decoders == 2 && probe.Attempts == 1
                && Diagnostics().GetProperty("decoderRecoveries").GetInt64() == 1,
                "native AAC recovery did not create one fresh decoder and speaker attempt");
        }
        finally { stop.Cancel(); await running.WaitAsync(TimeSpan.FromSeconds(3)); }
        Check(probe.Completed == 1 && !Diagnostics().GetProperty("ready").GetBoolean(),
            "native talk shutdown left its decoder or speaker alive");
    }

    public static async Task UnsupportedTalkDoesNotRetry()
    {
        var control = DispatchProxy.Create<ICameraControl, RecoveryControl>();
        var probe = (RecoveryControl)(object)control; probe.Failure = 3;
        var receiver = new BackchannelReceiver(control, TimeSpan.FromMilliseconds(10));
        receiver.Start(CancellationToken.None); receiver.OnPcm([1, 0]);
        await Until(() => probe.Attempts == 1);
        await Task.Delay(50); receiver.OnPcm([2, 0]);
        await Task.Delay(30);
        Check(probe.Attempts == 1, "unsupported speaker was retried continuously");
        await receiver.StopAsync();
    }
}

public class RecoveryControl : DispatchProxy
{
    public int Failure, Attempts, Completed, CapabilityReads;
    public Channel<byte[]> Pcm { get; } = Channel.CreateUnbounded<byte[]>();
    protected override object? Invoke(MethodInfo? method, object?[]? arguments)
    {
        if (method?.Name == "get_CameraName") return "synthetic-recovery";
        if (method?.Name == "GetCapabilitiesAsync")
        {
            Interlocked.Increment(ref CapabilityReads);
            return Task.FromResult(new CameraCapabilities(null, null, new CameraFeatures(false, false, false, false, true)));
        }
        if (method?.Name == "TalkAsync")
        {
            int attempt = Interlocked.Increment(ref Attempts);
            if (attempt == 1 && Failure >= 0)
            {
                Exception error = Failure switch
                {
                    0 => new TalkBusyException("synthetic-recovery"),
                    1 => new CameraOfflineException("synthetic-recovery"),
                    2 => new IOException("synthetic transport interruption"),
                    _ => new NotSupportedException("synthetic absent speaker")
                };
                return Task.FromException(error);
            }
            return Consume((ChannelReader<byte[]>)arguments![1]!, (CancellationToken)arguments[2]!);
        }
        throw new InvalidOperationException("Unexpected synthetic control method " + method?.Name);
    }
    private async Task Consume(ChannelReader<byte[]> input, CancellationToken ct)
    {
        try { await foreach (byte[] block in input.ReadAllAsync(ct)) Pcm.Writer.TryWrite(block); }
        finally { Interlocked.Increment(ref Completed); }
    }
}
