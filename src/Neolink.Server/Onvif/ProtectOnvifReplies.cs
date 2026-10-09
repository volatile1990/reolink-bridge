// Reolink Bridge read-only device and Media1/Media2 replies; AGPL-3.0.
using System.Security;
using Neolink.Streaming;

namespace Neolink.Onvif;

public sealed partial class ProtectOnvifServer
{
    private static string Esc(string text) => SecurityElement.Escape(text) ?? "";
    private static string Envelope(string body) => "<?xml version=\"1.0\" encoding=\"utf-8\"?>" +
        $"<s:Envelope xmlns:s=\"{NsSoap}\" xmlns:tt=\"{NsSchema}\" xmlns:tds=\"{NsDevice}\" xmlns:trt=\"{NsMedia}\" " +
        $"xmlns:tr2=\"{NsMedia2}\" xmlns:ter=\"http://www.onvif.org/ver10/error\"><s:Body>{body}</s:Body></s:Envelope>";
    private static (int Status, string Body) Ok(string body) => (200, Envelope(body));
    private static (int Status, string Body) Fault(int status, string subcode, string reason) => (status, Envelope(
        $"<s:Fault><s:Code><s:Value>s:Sender</s:Value><s:Subcode><s:Value>{subcode}</s:Value></s:Subcode></s:Code>" +
        $"<s:Reason><s:Text xml:lang=\"en\">{Esc(reason)}</s:Text></s:Reason></s:Fault>"));

    private static string SystemDateAndTime()
    {
        var now = DateTime.UtcNow;
        return "<tds:GetSystemDateAndTimeResponse><tds:SystemDateAndTime><tt:DateTimeType>NTP</tt:DateTimeType>" +
            "<tt:DaylightSavings>false</tt:DaylightSavings><tt:TimeZone><tt:TZ>UTC0</tt:TZ></tt:TimeZone>" +
            $"<tt:UTCDateTime><tt:Time><tt:Hour>{now.Hour}</tt:Hour><tt:Minute>{now.Minute}</tt:Minute><tt:Second>{now.Second}</tt:Second></tt:Time>" +
            $"<tt:Date><tt:Year>{now.Year}</tt:Year><tt:Month>{now.Month}</tt:Month><tt:Day>{now.Day}</tt:Day></tt:Date></tt:UTCDateTime>" +
            "</tds:SystemDateAndTime></tds:GetSystemDateAndTimeResponse>";
    }

    private string Services()
    {
        string Service(string ns, string path, int major, int minor) =>
            $"<tds:Service><tds:Namespace>{ns}</tds:Namespace><tds:XAddr>http://{_config.AdvertisedHost}:{_config.Port}{path}</tds:XAddr>" +
            $"<tds:Version><tt:Major>{major}</tt:Major><tt:Minor>{minor}</tt:Minor></tds:Version></tds:Service>";
        return "<tds:GetServicesResponse>" + Service(NsDevice, "/onvif/device_service", 2, 0) +
            Service(NsMedia, "/onvif/media_service", 2, 0) + Service(NsMedia2, "/onvif/media2_service", 2, 0) + "</tds:GetServicesResponse>";
    }

    private string Capabilities() => "<tds:GetCapabilitiesResponse><tds:Capabilities>" +
        $"<tt:Device><tt:XAddr>{_config.DeviceUrl}</tt:XAddr><tt:Network><tt:IPFilter>false</tt:IPFilter><tt:ZeroConfiguration>false</tt:ZeroConfiguration><tt:IPVersion6>false</tt:IPVersion6><tt:DynDNS>false</tt:DynDNS></tt:Network>" +
        "<tt:System><tt:DiscoveryResolve>true</tt:DiscoveryResolve><tt:DiscoveryBye>false</tt:DiscoveryBye><tt:RemoteDiscovery>false</tt:RemoteDiscovery>" +
        "<tt:SystemBackup>false</tt:SystemBackup><tt:SystemLogging>false</tt:SystemLogging><tt:FirmwareUpgrade>false</tt:FirmwareUpgrade></tt:System>" +
        "<tt:Security><tt:TLS1.1>false</tt:TLS1.1><tt:TLS1.2>false</tt:TLS1.2><tt:OnboardKeyGeneration>false</tt:OnboardKeyGeneration>" +
        "<tt:AccessPolicyConfig>false</tt:AccessPolicyConfig><tt:X.509Token>false</tt:X.509Token><tt:SAMLToken>false</tt:SAMLToken><tt:KerberosToken>false</tt:KerberosToken><tt:RELToken>false</tt:RELToken></tt:Security></tt:Device>" +
        $"<tt:Media><tt:XAddr>http://{_config.AdvertisedHost}:{_config.Port}/onvif/media_service</tt:XAddr><tt:StreamingCapabilities><tt:RTPMulticast>false</tt:RTPMulticast><tt:RTP_TCP>true</tt:RTP_TCP><tt:RTP_RTSP_TCP>true</tt:RTP_RTSP_TCP></tt:StreamingCapabilities></tt:Media>" +
        "</tds:Capabilities></tds:GetCapabilitiesResponse>";

    private static string DeviceServiceCapabilities() => "<tds:GetServiceCapabilitiesResponse><tds:Capabilities>" +
        "<tds:Network IPFilter=\"false\" ZeroConfiguration=\"false\" IPVersion6=\"false\" DynDNS=\"false\"/>" +
        "<tds:Security UsernameToken=\"true\" HttpDigest=\"false\"/><tds:System DiscoveryResolve=\"true\" DiscoveryBye=\"false\"/>" +
        "</tds:Capabilities></tds:GetServiceCapabilitiesResponse>";

    private string DeviceInformation() => "<tds:GetDeviceInformationResponse><tds:Manufacturer>Reolink Bridge</tds:Manufacturer>" +
        $"<tds:Model>{Esc(_config.Model)}</tds:Model><tds:FirmwareVersion>bridge-0.1</tds:FirmwareVersion>" +
        $"<tds:SerialNumber>{_config.SerialNumber}</tds:SerialNumber><tds:HardwareId>{Esc(_config.Uuid)}</tds:HardwareId></tds:GetDeviceInformationResponse>";

    private string NetworkInterfaces() => "<tds:GetNetworkInterfacesResponse><tds:NetworkInterfaces token=\"bridge0\"><tt:Enabled>true</tt:Enabled>" +
        $"<tt:Info><tt:Name>bridge0</tt:Name><tt:HwAddress>{_config.Mac}</tt:HwAddress><tt:MTU>1500</tt:MTU></tt:Info>" +
        $"<tt:IPv4><tt:Enabled>true</tt:Enabled><tt:Config><tt:Manual><tt:Address>{_config.AdvertisedHost}</tt:Address><tt:PrefixLength>24</tt:PrefixLength>" +
        "</tt:Manual><tt:DHCP>false</tt:DHCP></tt:Config></tt:IPv4></tds:NetworkInterfaces></tds:GetNetworkInterfacesResponse>";

    private string Scopes() => "<tds:GetScopesResponse>" + string.Concat(ScopeValues().Select(scope =>
        $"<tds:Scopes><tt:ScopeDef>Fixed</tt:ScopeDef><tt:ScopeItem>{Esc(scope)}</tt:ScopeItem></tds:Scopes>")) + "</tds:GetScopesResponse>";
    private string[] ScopeValues() => ["onvif://www.onvif.org/type/video_encoder", "onvif://www.onvif.org/Profile/Streaming",
        "onvif://www.onvif.org/name/" + Uri.EscapeDataString(_config.Name), "onvif://www.onvif.org/hardware/" + Uri.EscapeDataString(_config.Model)];

    private string MediaCapabilities(string prefix) => $"<{prefix}:GetServiceCapabilitiesResponse><{prefix}:Capabilities SnapshotUri=\"false\" Rotation=\"false\">" +
        $"<{prefix}:ProfileCapabilities MaximumNumberOfProfiles=\"{_streams.Count}\"/>" +
        $"<{prefix}:StreamingCapabilities RTPMulticast=\"false\" RTP_TCP=\"true\" RTP_RTSP_TCP=\"true\"/>" +
        $"</{prefix}:Capabilities></{prefix}:GetServiceCapabilitiesResponse>";

    private int Index(ProtectOnvifStream stream)
    {
        for (int i = 0; i < _streams.Count; i++) if (ReferenceEquals(_streams[i], stream)) return i;
        throw new InvalidOperationException("Unknown stream");
    }

    private (string Codec, uint Width, uint Height, int Fps, int Bitrate) Metadata(ProtectOnvifStream stream)
    {
        var configured = _config.Profiles[Index(stream)];
        return (stream.Hub.Codec?.ToString() ?? configured.Codec,
            stream.Hub.Width > 0 ? stream.Hub.Width : (uint)configured.Width,
            stream.Hub.Height > 0 ? stream.Hub.Height : (uint)configured.Height,
            configured.Fps, configured.Bitrate);
    }

    private string Source(string element, ProtectOnvifStream stream)
    {
        var m = Metadata(stream);
        return $"<{element} token=\"source_config_{Esc(stream.Token)}\"><tt:Name>{Esc(_config.Name)}</tt:Name><tt:UseCount>1</tt:UseCount>" +
            $"<tt:SourceToken>video_source</tt:SourceToken><tt:Bounds x=\"0\" y=\"0\" width=\"{m.Width}\" height=\"{m.Height}\"/></{element}>";
    }

    private string Encoder(string element, ProtectOnvifStream stream, bool media2)
    {
        var m = Metadata(stream);
        string codecSettings = m.Codec == "H264" ? $"<tt:H264><tt:GovLength>{m.Fps}</tt:GovLength><tt:H264Profile>Main</tt:H264Profile></tt:H264>" : "";
        string rate = media2 ? $"<tt:RateControl><tt:FrameRateLimit>{m.Fps}</tt:FrameRateLimit><tt:BitrateLimit>{m.Bitrate}</tt:BitrateLimit></tt:RateControl>"
            : $"<tt:RateControl><tt:FrameRateLimit>{m.Fps}</tt:FrameRateLimit><tt:EncodingInterval>1</tt:EncodingInterval><tt:BitrateLimit>{m.Bitrate}</tt:BitrateLimit></tt:RateControl>";
        // Media2's optional GovLength/Profile attributes are omitted: the relay has
        // not measured the encoder's GOP or profile, so fps must not stand in for GOP.
        return $"<{element} token=\"encoder_{Esc(stream.Token)}\">" +
            $"<tt:Name>{Esc(stream.Token)}</tt:Name><tt:UseCount>1</tt:UseCount><tt:Encoding>{m.Codec}</tt:Encoding>" +
            $"<tt:Resolution><tt:Width>{m.Width}</tt:Width><tt:Height>{m.Height}</tt:Height></tt:Resolution>" +
            (media2 ? rate : "<tt:Quality>5</tt:Quality>" + rate + codecSettings) +
            "<tt:Multicast><tt:Address><tt:Type>IPv4</tt:Type><tt:IPv4Address>0.0.0.0</tt:IPv4Address></tt:Address>" +
            "<tt:Port>0</tt:Port><tt:TTL>0</tt:TTL><tt:AutoStart>false</tt:AutoStart></tt:Multicast>" +
            (media2 ? "<tt:Quality>5</tt:Quality>" : "<tt:SessionTimeout>PT60S</tt:SessionTimeout>") + $"</{element}>";
    }

    private string Profile(string element, ProtectOnvifStream stream, bool media2) =>
        $"<{element} token=\"{Esc(stream.Token)}\" fixed=\"true\"><" + (media2 ? "tr2" : "tt") + $":Name>{Esc(_config.Name)} {Esc(stream.Token)}</" + (media2 ? "tr2" : "tt") + ":Name>" +
        (media2 ? "<tr2:Configurations>" + Source("tt:VideoSource", stream) + Encoder("tt:VideoEncoder", stream, true) + "</tr2:Configurations>"
            : Source("tt:VideoSourceConfiguration", stream) + Encoder("tt:VideoEncoderConfiguration", stream, false)) + $"</{element}>";

    private string VideoSources(string prefix)
    {
        var m = Metadata(_streams[0]);
        return $"<{prefix}:VideoSources token=\"video_source\"><tt:Framerate>{m.Fps}</tt:Framerate><tt:Resolution><tt:Width>{m.Width}</tt:Width>" +
            $"<tt:Height>{m.Height}</tt:Height></tt:Resolution></{prefix}:VideoSources>";
    }
}
