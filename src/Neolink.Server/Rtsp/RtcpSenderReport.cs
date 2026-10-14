// Compound SR/SDES for actual RTP tracks sharing one presentation clock. AGPL-3.0.
using System.Buffers.Binary;
using System.Text;

namespace Neolink.Rtsp;

internal static class RtcpSenderReport
{
    public static byte[] Build(RtpPacketizer track, uint rtpTimestamp, DateTimeOffset now, string cname)
    {
        byte[] name = Encoding.UTF8.GetBytes(cname);
        if (name.Length is < 1 or > 255) throw new ArgumentException("RTCP CNAME length");
        int sdesSize = (4 + 4 + 2 + name.Length + 1 + 3) & ~3;
        var data = new byte[28 + sdesSize]; var sr = data.AsSpan(0, 28);
        sr[0] = 0x80; sr[1] = 200; BinaryPrimitives.WriteUInt16BigEndian(sr[2..], 6);
        BinaryPrimitives.WriteUInt32BigEndian(sr[4..], track.Ssrc);
        long ticks = (now - new DateTimeOffset(1900, 1, 1, 0, 0, 0, TimeSpan.Zero)).Ticks;
        BinaryPrimitives.WriteUInt32BigEndian(sr[8..], unchecked((uint)(ticks / TimeSpan.TicksPerSecond)));
        BinaryPrimitives.WriteUInt32BigEndian(sr[12..], (uint)((ticks % TimeSpan.TicksPerSecond) * (double)(1L << 32) / TimeSpan.TicksPerSecond));
        BinaryPrimitives.WriteUInt32BigEndian(sr[16..], rtpTimestamp);
        BinaryPrimitives.WriteUInt32BigEndian(sr[20..], track.SentPackets);
        BinaryPrimitives.WriteUInt32BigEndian(sr[24..], track.SentPayloadOctets);
        var sdes = data.AsSpan(28); sdes[0] = 0x81; sdes[1] = 202;
        BinaryPrimitives.WriteUInt16BigEndian(sdes[2..], (ushort)(sdesSize / 4 - 1));
        BinaryPrimitives.WriteUInt32BigEndian(sdes[4..], track.Ssrc);
        sdes[8] = 1; sdes[9] = (byte)name.Length; name.CopyTo(sdes[10..]);
        return data;
    }
}
