using System.Reflection;
using System.Threading.Channels;
using Neolink.Media;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class AudioBackchannelContractTests
{
    private static void Check(bool ok, string message) { if (!ok) throw new Exception(message); }

    private static byte[] Packet(byte type = 0, params byte[] payload)
    {
        byte[] packet = new byte[12 + payload.Length];
        packet[0] = 0x80;
        packet[1] = type;
        payload.CopyTo(packet, 12);
        return packet;
    }

    public static Task StrictRtp()
    {
        byte[] payload = [0xD5, 0x55, 0xFF, 0x00];
        foreach (byte type in new byte[] { 0, 8 })
            Check(BackchannelReceiver.Depacketize(Packet(type, payload))!.SequenceEqual(
                G711.ToPcm16(payload, type == 8)), "G.711 payload does not decode exactly");

        byte[] decorated = new byte[12 + 8 + 4 + 8 + payload.Length + 4];
        decorated[0] = 0xB2; // v2, padding, extension, two CSRCs
        decorated[1] = 0x88; // marker + PCMA
        decorated[22] = 0; decorated[23] = 2; // two 32-bit extension words
        payload.CopyTo(decorated, 32);
        decorated[^1] = 4;
        Check(BackchannelReceiver.Depacketize(decorated)!.SequenceEqual(G711.ToPcm16(payload, true)),
            "CSRC, extension, marker or padding contaminated the decoded samples");

        var bad = new List<byte[]> { Array.Empty<byte>(), new byte[] { 0x80, 0 }, Packet(97, payload), Packet(72, payload), Packet(0) };
        byte[] csrc = Packet(0, payload); csrc[0] = 0x8F; bad.Add(csrc);
        byte[] shortExtension = Packet(0, 1); shortExtension[0] = 0x90; bad.Add(shortExtension);
        byte[] oversizedExtension = Packet(0, payload); oversizedExtension[0] = 0x90;
        oversizedExtension[14] = 0xFF; oversizedExtension[15] = 0xFF; bad.Add(oversizedExtension);
        byte[] zeroPadding = Packet(0, payload); zeroPadding[0] |= 0x20; zeroPadding[^1] = 0; bad.Add(zeroPadding);
        byte[] oversizedPadding = Packet(0, payload); oversizedPadding[0] |= 0x20;
        oversizedPadding[^1] = 5; bad.Add(oversizedPadding);
        byte[] paddingOnly = Packet(0, payload); paddingOnly[0] |= 0x20;
        paddingOnly[^1] = 4; bad.Add(paddingOnly);
        byte[] wrongVersion = Packet(0, payload); wrongVersion[0] = 0x40; bad.Add(wrongVersion);
        foreach (byte[] packet in bad)
            Check(BackchannelReceiver.Depacketize(packet) == null, "malformed or unsupported RTP became speaker audio");
        return Task.CompletedTask;
    }

    public static async Task LazySpeakerLifecycle()
    {
        var control = DispatchProxy.Create<ICameraControl, AudioControlProxy>();
        var probe = (AudioControlProxy)(object)control;
        var receiver = new BackchannelReceiver(control);
        byte[] stale = Packet(0, 0x00), fresh = Packet(0, 0xFF, 0x7F);
        receiver.OnRtp(stale);
        receiver.Start(CancellationToken.None);
        receiver.Start(CancellationToken.None);
        receiver.OnRtp(Packet(97, 1, 2, 3));
        // Allow any accidentally scheduled TalkAsync to execute, making the
        // assertion about PLAY/invalid RTP meaningful across scheduler speeds.
        await Task.Delay(50);
        Check(probe.Starts == 0, "PLAY, pre-PLAY or invalid RTP opened a speaker session");
        receiver.OnRtp(fresh);
        var first = await probe.Pcm.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(3));
        Check(probe.Starts == 1 && probe.SampleRate == 8000
            && first.SequenceEqual(G711.ToPcm16([0xFF, 0x7F], false)),
            "first valid packet was lost or a pre-PLAY packet was replayed");
        receiver.Stop();
        await probe.Ended.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(3));
        receiver.OnRtp(stale);
        receiver.Stop();
        receiver.Start(CancellationToken.None);
        await Task.Delay(30);
        Check(probe.Starts == 1 && !probe.Pcm.Reader.TryRead(out _), "closed-session RTP was retained");
        receiver.OnRtp(fresh);
        var second = await probe.Pcm.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(3));
        Check(probe.Starts == 2 && second.SequenceEqual(first), "rearmed receiver used a closed queue");
        receiver.Stop();
        await probe.Ended.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(3));
        using var cancelled = new CancellationTokenSource(); cancelled.Cancel();
        receiver.Start(cancelled.Token); receiver.OnRtp(fresh);
        await Task.Delay(30);
        Check(probe.Starts == 2, "cancelled lifetime opened a speaker session");
    }
}

/// <summary>A synthetic speaker sink. Any unrelated method is a test failure;
/// no camera, credential or device connection exists in these tests.</summary>
public class AudioControlProxy : DispatchProxy
{
    public int Starts;
    public int SampleRate;
    public Channel<byte[]> Pcm { get; } = Channel.CreateUnbounded<byte[]>();
    public Channel<bool> Ended { get; } = Channel.CreateUnbounded<bool>();

    protected override object? Invoke(MethodInfo? method, object?[]? args) => method?.Name switch
    {
        "get_CameraName" => "synthetic-audio-contract",
        "TalkAsync" => Consume((int)args![0]!, (ChannelReader<byte[]>)args[1]!, (CancellationToken)args[2]!),
        _ => throw new InvalidOperationException("Unexpected camera call in audio contract: " + method?.Name),
    };

    private async Task Consume(int rate, ChannelReader<byte[]> pcm, CancellationToken ct)
    {
        Interlocked.Increment(ref Starts); SampleRate = rate;
        try
        {
            await foreach (byte[] block in pcm.ReadAllAsync(ct))
                Pcm.Writer.TryWrite(block);
        }
        finally { Ended.Writer.TryWrite(true); }
    }
}
