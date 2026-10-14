using System.Text;
using System.Xml.Linq;
using Neolink.Config;
using Neolink.Media;
using Neolink.Onvif;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class OnvifAudioMetadataTests
{
    private static void Check(bool ok, string message) { if (!ok) throw new Exception(message); }
    private static void Key(StreamHub hub) => hub.PublishVideo(new(VideoCodec.H264, true, 0, null,
        [0, 0, 0, 1, 0x67, 66, 0, 30, 0, 0, 1, 0x68, 0xCE, 6, 0xE2, 0, 0, 1, 0x65, 1, 2, 3]));

    private static AacFrame Aac(int bytes = 512, int frequencyIndex = 8)
    {
        int length = 7 + bytes;
        byte[] data = new byte[length];
        data[0] = 0xFF; data[1] = 0xF1;
        data[2] = (byte)(0x40 | (frequencyIndex << 2));
        data[3] = (byte)(0x40 | (length >> 11));
        data[4] = (byte)(length >> 3); data[5] = (byte)((length << 5) | 0x1F); data[6] = 0xFC;
        Array.Fill(data, (byte)0x55, 7, bytes);
        return new(data);
    }

    private static ProtectOnvifServer Server(StreamHub main, StreamHub low, bool controls = true, bool gop = true) =>
        new(new BridgeOnvifConfig
        {
            Bind = "127.0.0.1", AdvertisedHost = "127.0.0.1", Port = 28080,
            Mac = "02:00:00:00:00:10", Uuid = "62d7d83c-2f33-4c1c-af64-226ee0c87d90", Events = false,
            Controls = controls, GopPlayout = gop,
            Profiles = [new() { Codec = "H264", Width = 1920, Height = 1080 },
                new() { Stream = "subStream", Codec = "H264", Width = 640, Height = 480 }],
        }, new Dictionary<string, string> { ["audio-contract"] = "synthetic-audio-test-only" },
        [new("main", "/synthetic/mainStream", main), new("sub", "/synthetic/subStream", low)], 28554);

    private static async Task<(int Status, XDocument Xml)> Reply(ProtectOnvifServer server, string action,
        bool media2 = false, string extra = "", bool authenticated = true)
    {
        string ns = media2 ? ProtectOnvifServer.NsMedia2 : ProtectOnvifServer.NsMedia;
        string envelope = $"<s:Envelope xmlns:s=\"{ProtectOnvifServer.NsSoap}\"><s:Body><m:{action} xmlns:m=\"{ns}\">" +
            extra + $"</m:{action}></s:Body></s:Envelope>";
        string? auth = authenticated ? "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(
            "audio-contract:synthetic-audio-test-only")) : null;
        var reply = await server.HandleAsync(envelope, auth, CancellationToken.None);
        return (reply.Status, XDocument.Parse(reply.Body));
    }

    public static async Task HonestProfileScope()
    {
        var main = new StreamHub("synthetic-main"); var low = new StreamHub("synthetic-low");
        Key(main); Key(low); main.PublishAac(Aac()); low.PublishAac(Aac());
        Check(low.AudioBitrateKbps == 64 && low.Audio?.SampleRate == 16000, "AAC metadata was guessed or includes ADTS overhead");
        var server = Server(main, low);
        foreach (bool media2 in new[] { false, true })
        {
            var reply = await Reply(server, "GetProfiles", media2);
            Check(reply.Status == 200, "audio profiles request failed");
            var profiles = reply.Xml.Descendants().Where(x => x.Name.LocalName == "Profiles").ToArray();
            Check(profiles.Length == 2, "audio metadata added a new video profile");
            var mainProfile = profiles.Single(x => (string?)x.Attribute("token") == "main");
            var lowProfile = profiles.Single(x => (string?)x.Attribute("token") == "sub");
            string sourceName = media2 ? "AudioSource" : "AudioSourceConfiguration";
            string encoderName = media2 ? "AudioEncoder" : "AudioEncoderConfiguration";
            Check(!mainProfile.Descendants().Any(x => x.Name.LocalName.StartsWith("Audio", StringComparison.Ordinal)),
                "GOP main advertised audio despite video-only RTP");
            Check(lowProfile.Descendants().Any(x => x.Name.LocalName == sourceName)
                && lowProfile.Descendants().Any(x => x.Name.LocalName == encoderName), "real low AAC did not appear in the profile");
            var encoder = lowProfile.Descendants().Single(x => x.Name.LocalName == encoderName);
            Check(encoder.Elements().Single(x => x.Name.LocalName == "Encoding").Value == (media2 ? "mpeg4-generic" : "AAC")
                && encoder.Elements().Single(x => x.Name.LocalName == "SampleRate").Value == "16"
                && encoder.Elements().Single(x => x.Name.LocalName == "Bitrate").Value == "64", "audio metadata disagrees with its RTP");
        }
        var configurations = await Reply(server, "GetAudioEncoderConfigurations");
        Check(configurations.Status == 200 && configurations.Xml.Descendants().Count(x => x.Name.LocalName == "Configurations") == 1,
            "GOP-main audio leaked into global audio configurations");
        var detail = await Reply(server, "GetAudioEncoderConfiguration", extra: "<m:ConfigurationToken>audio_encoder_sub</m:ConfigurationToken>");
        Check(detail.Status == 200, "known observed audio token is not readable");
        Check((await Reply(server, "GetAudioEncoderConfigurations", authenticated: false)).Status == 401,
            "audio metadata bypassed bridge authentication");
        Check((await Reply(server, "SetAudioEncoderConfiguration", extra: "<m:ConfigurationToken>audio_encoder_sub</m:ConfigurationToken>")).Status >= 400,
            "observed audio metadata became a camera write API");
        var source = await Reply(server, "GetAudioSources");
        Check(source.Status == 200 && source.Xml.Descendants().Single(x => x.Name.LocalName == "Channels").Value == "1",
            "physical microphone channels were not reported");
        Check(Sdp.Build(low, "synthetic-low").Contains("mpeg4-generic/16000/1"), "profile does not match actual AAC SDP");
        var defaults = await Reply(Server(main, low, controls: false), "GetProfiles");
        Check(!defaults.Xml.Descendants().Any(x => x.Name.LocalName.StartsWith("Audio", StringComparison.Ordinal)),
            "default bridge unexpectedly advertised new audio metadata");
    }

    public static async Task SourceEpochAndUnsupportedFormats()
    {
        var main = new StreamHub("synthetic-main"); var low = new StreamHub("synthetic-low");
        Key(main); Key(low); low.PublishAac(Aac(256)); low.PublishAac(Aac(512));
        Check(low.AudioBitrateKbps == 48, "AAC bitrate average did not use sample counts");
        var server = Server(main, low);
        low.SourceStopped(); Key(low);
        Check(!(await Reply(server, "GetProfiles")).Xml.Descendants().Any(x => x.Name.LocalName == "AudioEncoderConfiguration"),
            "new source epoch advertised old microphone metadata before any new audio");
        low.PublishAac(Aac());
        Check(low.AudioBitrateKbps == 64, "source epoch retained the old bitrate average");
        low.PublishAdpcm(new([0, 0, 0, 0, 0, 0]));
        Check(!(await Reply(server, "GetProfiles")).Xml.Descendants().Any(x => x.Name.LocalName == "AudioEncoderConfiguration"),
            "L16 PCM was falsely advertised as an ONVIF AAC/G711 codec");
        low.PublishAac(Aac(frequencyIndex: 4)); // 44.1 kHz cannot fit the integer-kHz AudioEncoder config
        Check(!(await Reply(server, "GetProfiles")).Xml.Descendants().Any(x => x.Name.LocalName == "AudioEncoderConfiguration"),
            "fractional-kHz sample rate was silently rounded to an inaccurate configuration");
        Check(low.Audio?.SampleRate == 44100, "source codec metadata did not follow the actual AAC header");
    }
}
