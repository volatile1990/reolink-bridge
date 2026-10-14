// Reolink Bridge: Protect-compatible, read-only virtual camera configuration.
// Licensed under AGPL-3.0; see LICENSE.
using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Neolink.Config;

public sealed class BridgeOnvifProfile
{
    public string Stream { get; set; } = "mainStream";
    public string Codec { get; set; } = "H265";
    public int Width { get; set; }
    public int Height { get; set; }
    public int Fps { get; set; } = 20;
    public int Bitrate { get; set; } = 10240;
}

public sealed class BridgeOnvifConfig
{
    public int Port { get; set; } = 8080;
    public string Bind { get; set; } = "0.0.0.0";
    public string AdvertisedHost { get; set; } = "";
    public string Mac { get; set; } = "";
    public string Uuid { get; set; } = "";
    public string Name { get; set; } = "Reolink Bridge";
    public string Model { get; set; } = "Baichuan camera";
    public bool Discovery { get; set; } = true;
    /// <summary>Forward actual camera alarm pushes through authenticated ONVIF PullPoint events.</summary>
    public bool Events { get; set; } = true;
    /// <summary>Opt-in authenticated control API. Disabled bridges retain their read-only behavior.</summary>
    public bool Controls { get; set; }
    /// <summary>Opt-in RTSP talk backchannel, requiring Controls and a discovered camera speaker.</summary>
    public bool Talk { get; set; }
    /// <summary>Optional native Protect UDP audio input, disabled at zero.</summary>
    public int NativeTalkPort { get; set; }
    /// <summary>Exact console source addresses permitted to send native talk media.</summary>
    public List<string> NativeTalkSenders { get; set; } = [];
    /// <summary>ONVIF motion forwards all activity, only camera classifications, or none. Class events and raw replay are unchanged.</summary>
    public string MotionEventPolicy { get; set; } = "all";
    /// <summary>Clear an active alarm if the camera stops sending alarm state, without interrupting video.</summary>
    public int EventStaleSeconds { get; set; } = 120;
    /// <summary>Opt-in complete-GOP lookahead and video-only outbound RTP normalization.</summary>
    public bool GopPlayout { get; set; }
    /// <summary>Opt-in original AAC on the complete-GOP presentation timeline. Requires Controls and GopPlayout.</summary>
    public bool GopAudio { get; set; }
    /// <summary>Additional startup reserve when GopPlayout is enabled.</summary>
    public int PlayoutDelayMs { get; set; }
    /// <summary>Explicit H264 substream to H265 conversion for Protect's matching HQ/LQ codec requirement.</summary>
    public string? LowQualityTranscode { get; set; }
    /// <summary>Actual native substream frame rate, used to clock raw input during low-quality conversion.</summary>
    public int LowQualitySourceFps { get; set; } = 20;
    /// <summary>Explicit low-quality encoder backend. Hardware failure never selects software implicitly.</summary>
    public string LowQualityEncoder { get; set; } = "software";
    /// <summary>Explicit low-quality decoder backend. VAAPI requires the VAAPI encoder and never falls back silently.</summary>
    public string LowQualityDecoder { get; set; } = "software";
    /// <summary>DRM render node used by the explicitly selected VAAPI decoder and encoder.</summary>
    public string LowQualityDevice { get; set; } = "/dev/dri/renderD128";
    /// <summary>Optional native H264 LQ VUI display ratio; encoded pictures remain unchanged.</summary>
    public string? LowQualitySampleAspectRatio { get; set; }
    public List<BridgeOnvifProfile> Profiles { get; set; } = new();
    /// <summary>Optional root-private persistent camera-push replay; unset keeps the bounded RAM-only mode.</summary>
    public string? EventReplayPath { get; set; }
    public int EventReplayRetentionSeconds { get; set; } = 86400;
    public int EventReplayMaxBytes { get; set; } = 8 * 1024 * 1024;
    public string? CameraHttpsCertificateSha256 { get; set; }
    /// <summary>Optional subset allowed to change camera settings; null preserves existing users' rights.</summary>
    public List<string>? ControlUsers { get; set; }

    public string EndpointReference => "urn:uuid:" + Guid.Parse(Uuid).ToString("D");
    public string SerialNumber => Guid.Parse(Uuid).ToString("N");
    public string DeviceUrl => $"http://{AdvertisedHost}:{Port}/onvif/device_service";

    public static BridgeOnvifConfig Parse(JsonElement element)
    {
        var result = new BridgeOnvifConfig();
        foreach (var property in element.EnumerateObject())
        {
            switch (Key(property.Name))
            {
                case "port": result.Port = property.Value.GetInt32(); break;
                case "bind": result.Bind = property.Value.GetString() ?? ""; break;
                case "advertisedhost": result.AdvertisedHost = property.Value.GetString() ?? ""; break;
                case "mac": result.Mac = property.Value.GetString() ?? ""; break;
                case "uuid": result.Uuid = property.Value.GetString() ?? ""; break;
                case "name": result.Name = property.Value.GetString() ?? ""; break;
                case "model": result.Model = property.Value.GetString() ?? ""; break;
                case "discovery": result.Discovery = property.Value.GetBoolean(); break;
                case "events": result.Events = property.Value.GetBoolean(); break;
                case "controls": result.Controls = property.Value.GetBoolean(); break;
                case "talk": result.Talk = property.Value.GetBoolean(); break;
                case "nativetalkport": result.NativeTalkPort = property.Value.GetInt32(); break;
                case "nativetalksenders": result.NativeTalkSenders = property.Value.EnumerateArray().Select(v => v.GetString() ?? "").ToList(); break;
                case "motioneventpolicy": result.MotionEventPolicy = property.Value.GetString() ?? ""; break;
                case "eventstaleseconds": result.EventStaleSeconds = property.Value.GetInt32(); break;
                case "playoutdelayms": result.PlayoutDelayMs = property.Value.GetInt32(); break;
                case "gopplayout": result.GopPlayout = property.Value.GetBoolean(); break;
                case "gopaudio": result.GopAudio = property.Value.GetBoolean(); break;
                case "lowqualitytranscode": result.LowQualityTranscode = property.Value.GetString(); break;
                case "lowqualitysourcefps": result.LowQualitySourceFps = property.Value.GetInt32(); break;
                case "lowqualityencoder": result.LowQualityEncoder = property.Value.GetString() ?? ""; break;
                case "lowqualitydecoder": result.LowQualityDecoder = property.Value.GetString() ?? ""; break;
                case "lowqualitydevice": result.LowQualityDevice = property.Value.GetString() ?? ""; break;
                case "lowqualitysampleaspectratio": result.LowQualitySampleAspectRatio = property.Value.GetString(); break;
                case "profiles":
                    foreach (var item in property.Value.EnumerateArray())
                    {
                        var profile = new BridgeOnvifProfile();
                        foreach (var part in item.EnumerateObject())
                            switch (Key(part.Name))
                            {
                                case "stream": profile.Stream = part.Value.GetString() ?? ""; break;
                                case "codec": profile.Codec = part.Value.GetString() ?? ""; break;
                                case "width": profile.Width = part.Value.GetInt32(); break;
                                case "height": profile.Height = part.Value.GetInt32(); break;
                                case "fps": profile.Fps = part.Value.GetInt32(); break;
                                case "bitrate": profile.Bitrate = part.Value.GetInt32(); break;
                                default: throw new FormatException("Unknown ONVIF profile option");
                            }
                        result.Profiles.Add(profile);
                    }
                    break;
                case "eventreplaypath": result.EventReplayPath = property.Value.GetString(); break;
                case "eventreplayretentionseconds": result.EventReplayRetentionSeconds = property.Value.GetInt32(); break;
                case "eventreplaymaxbytes": result.EventReplayMaxBytes = property.Value.GetInt32(); break;
                case "camerahttpscertificatesha256": result.CameraHttpsCertificateSha256 = property.Value.GetString(); break;
                case "controlusers": result.ControlUsers = property.Value.EnumerateArray().Select(v => v.GetString() ?? "").ToList(); break;
                default: throw new FormatException("Unknown ONVIF option");
            }
        }
        result.Validate();
        return result;
    }

    public void Validate()
    {
        if (EventReplayRetentionSeconds is < 60 or > 604800 || EventReplayMaxBytes is < 65536 or > 67108864)
            throw new FormatException("Event replay retention must be 60-604800 seconds and its byte budget 64 KiB-64 MiB");
        if (EventReplayPath != null && (!Events || !Path.IsPathFullyQualified(EventReplayPath)
            || EventReplayPath.Length > 512 || EventReplayPath.Any(char.IsControl)))
            throw new FormatException("Persistent replay requires events and an explicit absolute local file path");
        if (CameraHttpsCertificateSha256 != null && (!Controls || !Regex.IsMatch(CameraHttpsCertificateSha256, "^[a-fA-F0-9]{64}$")))
            throw new FormatException("Camera HTTPS pin requires controls and exactly 64 SHA256 hex characters");
        if (ControlUsers != null && (!Controls || ControlUsers.Distinct(StringComparer.Ordinal).Count() != ControlUsers.Count
            || ControlUsers.Any(string.IsNullOrWhiteSpace)))
            throw new FormatException("Control users must be a distinct subset of authenticated bridge users");
        if (NativeTalkPort > 0 && ControlUsers is { Count: 0 })
            throw new FormatException("Native talk requires at least one authorized control user");
        if (Talk && !Controls) throw new FormatException("onvif.talk requires onvif.controls");
        if (GopAudio && (!Controls || !GopPlayout)) throw new FormatException("onvif.gop_audio requires controls and gop_playout");
        if (NativeTalkPort is < 0 or > 65535 || (NativeTalkPort > 0 && (!Talk || NativeTalkPort == Port)))
            throw new FormatException("Native talk requires enabled controls/talk and a distinct UDP port 1-65535");
        if (NativeTalkPort > 0 && (NativeTalkSenders.Count is < 1 or > 4 || NativeTalkSenders.Distinct().Count() != NativeTalkSenders.Count))
            throw new FormatException("Native talk requires one to four distinct console IPv4 source addresses");
        foreach (string sender in NativeTalkSenders)
            if (!IPAddress.TryParse(sender, out var ip) || ip.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork
                || ip.Equals(IPAddress.Any) || ip.Equals(IPAddress.Broadcast) || ip.GetAddressBytes()[0] >= 224)
                throw new FormatException("Native talk sender must be an explicit unicast IPv4 address");
        if (Port is < 1 or > 65535) throw new FormatException("onvif.port must be between 1 and 65535");
        if (PlayoutDelayMs is < 0 or > 5000) throw new FormatException("onvif.playout_delay_ms must be between 0 and 5000");
        if (EventStaleSeconds is < 5 or > 600) throw new FormatException("onvif.event_stale_seconds must be between 5 and 600");
        if (MotionEventPolicy is not ("all" or "classified" or "none"))
            throw new FormatException("onvif.motion_event_policy must be all, classified or none");
        if (!IPAddress.TryParse(Bind, out var bind) || bind.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork)
            throw new FormatException("onvif.bind must be an IPv4 address");
        if (!IPAddress.TryParse(AdvertisedHost, out var host) || host.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork
            || host.Equals(IPAddress.Any) || host.Equals(IPAddress.Broadcast))
            throw new FormatException("onvif.advertised_host must be the virtual camera's reachable IPv4 address");
        if (!Guid.TryParse(Uuid, out var uuid) || uuid == Guid.Empty)
            throw new FormatException("onvif.uuid must be a persistent, nonempty UUID");
        if (!Regex.IsMatch(Mac, "^[0-9a-fA-F]{2}(:[0-9a-fA-F]{2}){5}$") || (Convert.ToByte(Mac[..2], 16) & 1) != 0
            || Mac.Replace(":", "").All(c => c == '0'))
            throw new FormatException("onvif.mac must be a valid unicast MAC address matching the virtual interface");
        Mac = Mac.ToLowerInvariant();
        Uuid = uuid.ToString("D");
        if (string.IsNullOrWhiteSpace(Name) || Name.Length > 128 || string.IsNullOrWhiteSpace(Model) || Model.Length > 128)
            throw new FormatException("onvif.name and model must contain 1 to 128 characters");
        if (Profiles.Count is < 1 or > 3 || Profiles.Select(p => p.Stream).Distinct().Count() != Profiles.Count)
            throw new FormatException("onvif.profiles must contain one to three distinct streams");
        foreach (var p in Profiles)
        {
            if (p.Stream is not ("mainStream" or "subStream" or "externStream") || p.Codec is not ("H264" or "H265")
                || p.Width is < 1 or > 16384 || p.Height is < 1 or > 16384 || p.Fps is < 1 or > 120 || p.Bitrate is < 1 or > 100000)
                throw new FormatException("Invalid ONVIF profile: provide stream, actual H264/H265 codec, resolution, fps and bitrate");
        }
        if (LowQualityTranscode != null && LowQualityTranscode != "H265")
            throw new FormatException("onvif.low_quality_transcode must be H265 when specified");
        if (LowQualitySourceFps is < 1 or > 120)
            throw new FormatException("onvif.low_quality_source_fps must be between 1 and 120");
        if (LowQualityEncoder is not ("software" or "vaapi"))
            throw new FormatException("onvif.low_quality_encoder must be software or vaapi");
        if (LowQualityDecoder is not ("software" or "vaapi"))
            throw new FormatException("onvif.low_quality_decoder must be software or vaapi");
        if (LowQualityDecoder == "vaapi" && (LowQualityEncoder != "vaapi" || LowQualityTranscode != "H265"))
            throw new FormatException("VAAPI decoding requires explicit VAAPI H265 low-quality conversion");
        if (!Regex.IsMatch(LowQualityDevice, "^/dev/dri/renderD[0-9]{1,4}$"))
            throw new FormatException("onvif.low_quality_device must be an absolute DRM render node");
        if (LowQualityEncoder == "vaapi" && LowQualityTranscode == null)
            throw new FormatException("VAAPI requires explicit H265 low-quality conversion");
        if (LowQualityTranscode != null && (Profiles.Count != 2 || Profiles[0].Stream != "mainStream"
            || Profiles[1].Stream != "subStream" || Profiles.Any(p => p.Codec != "H265")))
            throw new FormatException("H265 low-quality conversion requires mainStream and subStream H265 output profiles");
        if (LowQualityTranscode != null && (Profiles[1].Fps > 30 || LowQualitySourceFps > 60
            || Profiles[1].Bitrate is < 64 or > 8192))
            throw new FormatException("H265 low-quality conversion requires output fps 1-30, native fps 1-60 and bitrate 64-8192 kbit/s");
        if (LowQualityTranscode != null && (Profiles[1].Width % 2 != 0 || Profiles[1].Height % 2 != 0))
            throw new FormatException("H265 low-quality output width and height must be even for 4:2:0 encoding");
        if (LowQualitySampleAspectRatio is not (null or "4:3"))
            throw new FormatException("onvif.low_quality_sample_aspect_ratio must be 4:3 when specified");
        if (LowQualitySampleAspectRatio != null && (LowQualityTranscode != null || Profiles.Count != 2
            || Profiles[0].Stream != "mainStream" || Profiles[1].Stream != "subStream"
            || Profiles.Any(p => p.Codec != "H264") || Profiles[1].Width * 3 != Profiles[1].Height * 4))
            throw new FormatException("Low-quality sample aspect ratio requires native H264 main/sub and a 4:3 sub picture");
    }

    private static string Key(string name) => name.Replace("_", "").Replace("-", "").ToLowerInvariant();
}
