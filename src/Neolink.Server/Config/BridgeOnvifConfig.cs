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
    public List<BridgeOnvifProfile> Profiles { get; set; } = new();

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
                default: throw new FormatException("Unknown ONVIF option");
            }
        }
        result.Validate();
        return result;
    }

    public void Validate()
    {
        if (Port is < 1 or > 65535) throw new FormatException("onvif.port must be between 1 and 65535");
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
    }

    private static string Key(string name) => name.Replace("_", "").Replace("-", "").ToLowerInvariant();
}
