// Reolink Bridge: RTP picture boundaries without dropping parameter/SEI NALs; AGPL-3.0.
using Neolink.Media;

namespace Neolink.Rtsp;

internal sealed record RtpAccessUnit(IReadOnlyList<ReadOnlyMemory<byte>> Nals, bool HasVcl, bool Keyframe, bool HasSps);

/// <summary>The same first-slice boundary estimates as FMp4.SplitAccessUnitsRaw,
/// but retains parameter sets, AUDs and SEI for RTP. Metadata never creates a picture.
/// These are boundary estimates, not a decoder or a complete H264 picture-header parser.</summary>
internal static class RtpAccessUnitSplitter
{
    public static List<RtpAccessUnit> Split(VideoCodec codec, byte[] annexB)
    {
        var result = new List<RtpAccessUnit>();
        var group = new List<ReadOnlyMemory<byte>>();
        bool hasVcl = false, keyframe = false, hasSps = false;
        void Close()
        {
            if (group.Count == 0) return;
            result.Add(new RtpAccessUnit(group, hasVcl, keyframe, hasSps));
            group = new(); hasVcl = keyframe = hasSps = false;
        }
        foreach (var nal in H26x.SplitNals(annexB))
        {
            var span = nal.Span;
            if (span.Length == 0) continue;
            int type = codec == VideoCodec.H265 ? H26x.H265NalType(span) : H26x.H264NalType(span);
            bool vcl = codec == VideoCodec.H265 ? span.Length >= 3 && type <= 31 : span.Length >= 2 && type is >= 1 and <= 5;
            bool first = vcl && (span[codec == VideoCodec.H265 ? 2 : 1] & 0x80) != 0;
            bool prefix = codec == VideoCodec.H265 ? type is 32 or 33 or 34 or 35 or 39 : type is 6 or 7 or 8 or 9;
            if (hasVcl && (prefix || first)) Close();
            group.Add(nal);
            hasVcl |= vcl;
            keyframe |= vcl && (codec == VideoCodec.H265 ? type is >= 16 and <= 23 : type == 5);
            hasSps |= codec == VideoCodec.H265 ? type == 33 : type == 7;
        }
        Close();
        return result;
    }

    public static byte[] AnnexB(IReadOnlyList<ReadOnlyMemory<byte>> nals)
    {
        var output = new byte[nals.Sum(nal => checked(4 + nal.Length))];
        int offset = 0;
        foreach (var nal in nals)
        {
            output[offset + 3] = 1;
            nal.Span.CopyTo(output.AsSpan(offset + 4));
            offset += 4 + nal.Length;
        }
        return output;
    }
}
