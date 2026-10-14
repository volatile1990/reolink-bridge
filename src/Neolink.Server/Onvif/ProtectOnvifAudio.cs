// Reolink Bridge observed AAC media metadata; AGPL-3.0.
using System.Xml.Linq;
using Neolink.Streaming;

namespace Neolink.Onvif;

public sealed partial class ProtectOnvifServer
{
    // The original bridge profile is unchanged unless controls are explicitly
    // enabled. GOP-main aliases remain video-only. The existing non-GOP output
    // can advertise real AAC once its source has actually published that audio.
    private sealed record ObservedAudio(AudioTrackInfo Track, int Bitrate);

    private ObservedAudio? CurrentAudio(ProtectOnvifStream stream)
    {
        if (!_config.Controls || (_config.GopPlayout && Index(stream) == 0 && !_config.GopAudio)
            || !stream.Hub.LiveVideo || stream.Hub is not StreamHub hub
            || hub.AudioMetadataSourceEpoch != hub.SourceEpoch)
            return null;
        if (_config.GopPlayout && Index(stream) == 0 && (hub.LastAudioAgeMs is not (>= 0 and <= 5000)
            || hub.GetVideoDiagnostics().LastVideoAgeMs is not (>= 0 and <= 5000)
            || hub.Audio is not { Channels: >= 1 and <= 2, SampleRate: >= 8000 and <= 96000, AudioSpecificConfig.Length: >= 2 })) return null;
        var bitrate = hub.AudioBitrateKbps;
        return bitrate is > 0 && hub.Audio is { IsAac: true, SampleRate: > 0, Channels: > 0 } audio
            && audio.SampleRate % 1000 == 0 ? new(audio, bitrate.Value) : null;
    }

    private string AudioSource(string element, ProtectOnvifStream stream) => CurrentAudio(stream) == null ? "" :
        $"<{element} token=\"audio_source_config_{Esc(stream.Token)}\"><tt:Name>{Esc(_config.Name)} audio</tt:Name>" +
        "<tt:UseCount>1</tt:UseCount><tt:SourceToken>audio_source</tt:SourceToken>" + $"</{element}>";

    private string AudioEncoder(string element, ProtectOnvifStream stream, bool media2)
    {
        var audio = CurrentAudio(stream);
        if (audio == null) return "";
        int bitrate = audio.Bitrate;
        string multicast = "<tt:Multicast><tt:Address><tt:Type>IPv4</tt:Type><tt:IPv4Address>0.0.0.0</tt:IPv4Address>" +
            "</tt:Address><tt:Port>0</tt:Port><tt:TTL>0</tt:TTL><tt:AutoStart>false</tt:AutoStart></tt:Multicast>";
        string rate = $"<tt:Bitrate>{bitrate}</tt:Bitrate><tt:SampleRate>{audio.Track.SampleRate / 1000}</tt:SampleRate>";
        // Media1 uses its AAC enum; Media2 uses the actual SDP media subtype.
        // Their schema ordering differs (Multicast precedes Bitrate in Media2).
        return $"<{element} token=\"audio_encoder_{Esc(stream.Token)}\"><tt:Name>{Esc(stream.Token)} audio</tt:Name>" +
            $"<tt:UseCount>1</tt:UseCount><tt:Encoding>{(media2 ? "mpeg4-generic" : "AAC")}</tt:Encoding>" +
            (media2 ? multicast + rate : rate + multicast + "<tt:SessionTimeout>PT60S</tt:SessionTimeout>") +
            $"</{element}>";
    }

    private bool TryAudioReply(string action, string prefix, bool media2, XElement op,
        out (int Status, string Body) reply)
    {
        reply = default;
        if (action is not ("GetAudioSources" or "GetAudioSourceConfigurations" or "GetAudioEncoderConfigurations"
            or "GetAudioSourceConfiguration" or "GetAudioEncoderConfiguration")) return false;
        // Media2 has no GetAudioSources operation; its physical source is read
        // through Media1 rather than inventing a nonstandard response.
        if (media2 && action == "GetAudioSources")
        {
            reply = Fault(400, "ter:ActionNotSupported", "Read audio sources through the Media1 service");
            return true;
        }
        var streams = _streams.Where(s => CurrentAudio(s) != null).ToArray();
        if (action == "GetAudioSources")
        {
            int channels = streams.Select(s => CurrentAudio(s)?.Track.Channels ?? 0).DefaultIfEmpty(0).Max();
            reply = Ok($"<{prefix}:GetAudioSourcesResponse>" + (channels == 0 ? "" :
                $"<{prefix}:AudioSources token=\"audio_source\"><tt:Channels>{channels}</tt:Channels></{prefix}:AudioSources>") +
                $"</{prefix}:GetAudioSourcesResponse>");
            return true;
        }
        bool encoder = action.Contains("Encoder", StringComparison.Ordinal);
        string? token = Child(op, "ConfigurationToken");
        if (action.EndsWith("Configurations", StringComparison.Ordinal))
        {
            if (token != null) streams = streams.Where(s => token == (encoder ? "audio_encoder_" : "audio_source_config_") + s.Token).ToArray();
            reply = Ok($"<{prefix}:{action}Response>" + string.Concat(streams.Select(s => encoder
                ? AudioEncoder(prefix + ":Configurations", s, media2) : AudioSource(prefix + ":Configurations", s))) +
                $"</{prefix}:{action}Response>");
            return true;
        }
        var selected = streams.FirstOrDefault(s => token == (encoder ? "audio_encoder_" : "audio_source_config_") + s.Token);
        reply = selected == null ? Fault(400, "ter:InvalidArgVal", "Unknown or unavailable audio configuration token")
            : Ok($"<{prefix}:{action}Response>" + (encoder ? AudioEncoder(prefix + ":Configuration", selected, media2)
                : AudioSource(prefix + ":Configuration", selected)) + $"</{prefix}:{action}Response>");
        return true;
    }
}
