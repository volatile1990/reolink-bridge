using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using Neolink.Media;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class GopNalPacketizationTests
{
    private static void Check(bool value, string message) { if (!value) throw new Exception(message); }
    private static byte[] Annex(params byte[][] nals) => nals.SelectMany(n => new byte[] { 0, 0, 0, 1 }.Concat(n)).ToArray();
    private static byte[] Nal(VideoCodec codec, int length, bool key = false)
    {
        var bytes = Enumerable.Repeat((byte)0xa5, length).ToArray();
        bytes[0] = codec == VideoCodec.H264 ? key ? (byte)0x65 : (byte)0x41 : key ? (byte)0x26 : (byte)0x02;
        if (codec == VideoCodec.H265) bytes[1] = 1;
        bytes[codec == VideoCodec.H264 ? 1 : 2] = 0x80;
        return bytes;
    }
    private static void Fix(RtpPacketizer p)
    {
        typeof(RtpPacketizer).GetField("<Ssrc>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(p, 0x31415926u);
        typeof(RtpPacketizer).GetField("<Seq>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(p, (ushort)65530);
    }
    private static void Equal(RtpBatch before, RtpBatch after)
    {
        Check(before.Framed.Span.SequenceEqual(after.Framed.Span), "NAL path changed TCP framing, RTP header, payload or marker");
        Check(before.Count == after.Count, "NAL path changed RTP packet count");
        for (int i = 0; i < before.Count; i++) Check(before.PacketAt(i).Span.SequenceEqual(after.PacketAt(i).Span), "NAL path changed a UDP datagram");
    }
    public static Task WireParityAndOwnership()
    {
        foreach (var codec in Enum.GetValues<VideoCodec>())
        {
            var before = new RtpPacketizer(96); var after = new RtpPacketizer(96); Fix(before); Fix(after);
            var bb = new RtpBatch(); var cb = new RtpBatch();
            foreach (int length in new[] { 3, 1399, 1400, 1401, 2798, 2800, 2801, 90_001 })
            {
                // Mixed start codes, empty NALs, padding, multiple slices and suffix metadata.
                byte[] input = new byte[] { 0, 0, 1 }.Concat(Nal(codec, length, true)).Concat(new byte[] { 0, 0, 0, 1 })
                    .Concat(new byte[] { 0, 0, 1 }).Concat(Nal(codec, 13)).Concat(new byte[] { 0, 0, 0 }).ToArray();
                var units = RtpAccessUnitSplitter.Split(codec, input);
                var nals = units.SelectMany(unit => unit.Nals).ToArray();
                Check(nals.All(n => MemoryMarshal.TryGetArray(n, out var array) && ReferenceEquals(array.Array, input)),
                    "Splitter copied a source NAL payload");
                byte[] canonical = RtpAccessUnitSplitter.AnnexB(nals);
                uint timestamp = unchecked(uint.MaxValue - (uint)length);
                before.PacketizeVideoInto(bb, codec, canonical, timestamp, 3);
                after.PacketizeVideoNalsInto(cb, codec, nals, timestamp, 3);
                Equal(bb, cb);
            }
            // A later reset/reuse of the batch cannot alter retained source slices.
            byte[] source = Annex(Nal(codec, 3001, true)); var retained = H26x.SplitNals(source);
            byte[] snapshot = retained[0].ToArray();
            after.PacketizeVideoNalsInto(cb, codec, retained, 10, 0);
            after.PacketizeVideoInto(cb, codec, Annex(Nal(codec, 4000)), 20, 2);
            Check(retained[0].Span.SequenceEqual(snapshot), "Batch reuse mutated a source payload");
        }
        return Task.CompletedTask;
    }

    public static Task BoundsLifetimeAndSessions()
    {
        foreach (var codec in Enum.GetValues<VideoCodec>())
        {
            var one = new GopVideoPlayout(codec, 0); var two = new GopVideoPlayout(codec, 0);
            byte[] prefix = Annex(codec == VideoCodec.H264 ? new byte[] { 0x67, 0x80 } : new byte[] { 0x42, 1, 0x80 });
            byte[] bundled = Annex(Nal(codec, 1701, true), Nal(codec, 3101));
            byte[] tail = Annex(codec == VideoCodec.H264 ? new byte[] { 0x06, 0x80 } : new byte[] { 0x4e, 1, 0x80 });
            var sources = new[] { prefix, bundled, tail };
            HubVideo Frame(byte[] bytes, int index, long arrival, bool key) => new(index, bytes, key, (uint)(index * 4500))
                { ArrivalTimestamp = arrival, ArrivalTimestampFrequency = 1_000_000, SourceEpoch = 7 };
            foreach (var frame in new[] { Frame(prefix, 0, 0, false), Frame(bundled, 0, 0, true), Frame(tail, 1, 50_000, false) })
            { one.Push(frame); two.Push(frame); }
            var closing = Frame(Annex(Nal(codec, 11, true)), 2, 100_000, true);
            var first = one.Push(closing)!; var other = two.Push(closing)!;
            Check(first.Pictures.Count == 2 && other.Pictures.Count == 2, "Independent sessions changed bundled picture count");
            var oldPayloads = first.Pictures.Select(p => RtpAccessUnitSplitter.AnnexB(p.Nals)).ToArray();
            Check(first.Pictures.SelectMany(p => p.Nals).All(n => MemoryMarshal.TryGetArray(n, out var array)
                && sources.Any(source => ReferenceEquals(source, array.Array))), "Completed GOP copied or lost ownership of a source payload");
            one.Reset();
            one.Push(Frame(Annex(Nal(codec, 99, true)), 3, 150_000, true) with { SourceEpoch = 8 });
            one.Push(Frame(Annex(Nal(codec, 101, true)), 4, 200_000, true) with { SourceEpoch = 8 });
            for (int i = 0; i < first.Pictures.Count; i++)
                Check(RtpAccessUnitSplitter.AnnexB(first.Pictures[i].Nals).SequenceEqual(oldPayloads[i])
                    && RtpAccessUnitSplitter.AnnexB(other.Pictures[i].Nals).SequenceEqual(oldPayloads[i]),
                    "Reset/new epoch changed a completed GOP or another session");
        }
        // Raw bytes fit exactly but converting two 3-byte start codes to canonical
        // 4-byte start codes exceeds the old output budget. It must still reject.
        const int cap = 6 * 1024 * 1024;
        var raw = new byte[cap]; raw.AsSpan().Fill(0xa5);
        raw[0] = raw[1] = 0; raw[2] = 1; raw[3] = 0x65; raw[4] = 0x80;
        raw[cap - 7] = raw[cap - 6] = 0; raw[cap - 5] = 1; raw[cap - 4] = 0x41; raw[cap - 3] = 0x80;
        var budget = new GopVideoPlayout(VideoCodec.H264, 0);
        budget.Push(new(0, raw, true, 0) { ArrivalTimestamp = 0, ArrivalTimestampFrequency = 1_000_000 });
        Check(budget.BufferedBytes == cap && budget.BufferedFrames == 2, "Raw byte/picture accounting changed");
        Check(budget.Push(new(1, Annex(Nal(VideoCodec.H264, 4, true)), true, 9000)
            { ArrivalTimestamp = 100_000, ArrivalTimestampFrequency = 1_000_000 }) == null,
            "NAL path bypassed the canonical 6MiB output budget");
        return Task.CompletedTask;
    }

    public static async Task MissingParameterFallback()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
        using var peer = new TcpClient(); var connect = peer.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);
        using var socket = await listener.AcceptTcpClientAsync(); await connect;
        var server = new RtspServer(new Dictionary<string, string>());
        var conn = new RtspConnection(socket, server);
        var oldSend = typeof(RtspSession).GetMethod("SendVideoAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var newSend = typeof(RtspSession).GetMethod("SendVideoNalsAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        foreach (var codec in Enum.GetValues<VideoCodec>())
        {
            var hub = new StreamHub("synthetic-nal-prefix"); hub.PublishInfo(new(1920, 1080, 20));
            hub.PublishVideo(new(codec, true, 0, null, Annex(Nal(codec, 9, true))));
            foreach (byte[]? parameter in new byte[]?[] { null, [], [0], [0, 0, 0], [0x42, 1, 0x80, 0, 0], [0x42, 1, 0, 0, 1, 0x44, 1, 0x80] })
            {
                var description = new StreamDescriptionSnapshot(codec, parameter, parameter, parameter, 1920, 1080, null, 0, true, true);
                var session = new RtspSession(conn, new() { Path = "/synthetic", Hub = hub, GopPlayout = true }, false, description);
                var p = new RtpPacketizer(96); session.SetTrack(0, new() { Packetizer = p });
                var nals = H26x.SplitNals(Annex(Nal(codec, 1401, true)));
                foreach (var flags in new[] { (Key: true, Sps: false), (Key: true, Sps: true), (Key: false, Sps: false) })
                {
                    var before = new RtpBatch(); var after = new RtpBatch(); Fix(p);
                    await (Task)oldSend.Invoke(session, new object[] { hub, RtpAccessUnitSplitter.AnnexB(nals), flags.Key, flags.Sps, 123u, before, CancellationToken.None })!;
                    Fix(p);
                    await (Task)newSend.Invoke(session, new object[] { hub, nals, flags.Key, flags.Sps, 123u, after, CancellationToken.None })!;
                    Equal(before, after);
                }
            }
        }
    }
}
