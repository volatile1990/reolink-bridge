// Reolink Bridge WS-Discovery; AGPL-3.0.
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace Neolink.Onvif;

public sealed partial class ProtectOnvifServer
{
    private const string NsDiscovery = "http://schemas.xmlsoap.org/ws/2005/04/discovery";
    private const string NsAddressing = "http://schemas.xmlsoap.org/ws/2004/08/addressing";
    private async Task RunDiscoveryAsync(CancellationToken ct)
    {
        using var udp = new UdpClient(AddressFamily.InterNetwork);
        try
        {
            udp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            udp.Client.Bind(new IPEndPoint(IPAddress.Any, 3702));
            udp.JoinMulticastGroup(IPAddress.Parse("239.255.255.250"), IPAddress.Parse(_config.AdvertisedHost));
            Log.Info("ONVIF discovery listening on the virtual camera interface");
            while (!ct.IsCancellationRequested)
            {
                var request = await udp.ReceiveAsync(ct).ConfigureAwait(false);
                if (request.Buffer.Length > 8192) continue;
                XElement envelope;
                try { envelope = ParseXml(Encoding.UTF8.GetString(request.Buffer)); }
                catch (XmlException) { continue; }
                var body = envelope.Elements().FirstOrDefault(e => e.Name.LocalName == "Body");
                var operation = body?.Elements().FirstOrDefault();
                if (operation?.Name.NamespaceName != NsDiscovery || operation.Name.LocalName is not ("Probe" or "Resolve")) continue;
                if (operation.Name.LocalName == "Resolve" && operation.Descendants().FirstOrDefault(e => e.Name.LocalName == "Address")?.Value != _config.EndpointReference) continue;
                // Ignore probes specifically for other device classes; accepted scopes are ours only.
                var types = Child(operation, "Types");
                if (!string.IsNullOrWhiteSpace(types) && !types.Split(' ').Any(t => t.EndsWith(":NetworkVideoTransmitter", StringComparison.Ordinal))) continue;
                var scopes = Child(operation, "Scopes");
                if (!string.IsNullOrWhiteSpace(scopes) && scopes.Split(' ', StringSplitOptions.RemoveEmptyEntries).Any(s => !ScopeValues().Any(ours => ours.StartsWith(s, StringComparison.Ordinal)))) continue;
                var relates = envelope.Descendants().FirstOrDefault(e => e.Name.LocalName == "MessageID")?.Value;
                string match = operation.Name.LocalName == "Probe" ? "ProbeMatch" : "ResolveMatch";
                string action = operation.Name.LocalName == "Probe" ? "ProbeMatches" : "ResolveMatches";
                string response = $"<s:Envelope xmlns:s=\"{NsSoap}\" xmlns:a=\"{NsAddressing}\" xmlns:d=\"{NsDiscovery}\" xmlns:dn=\"http://www.onvif.org/ver10/network/wsdl\"><s:Header>" +
                    $"<a:MessageID>urn:uuid:{Guid.NewGuid():D}</a:MessageID><a:RelatesTo>{Esc(relates ?? "")}</a:RelatesTo>" +
                    $"<a:To>{NsAddressing}/role/anonymous</a:To><a:Action>{NsDiscovery}/{action}</a:Action></s:Header><s:Body><d:{action}><d:{match}>" +
                    $"<a:EndpointReference><a:Address>{_config.EndpointReference}</a:Address></a:EndpointReference><d:Types>dn:NetworkVideoTransmitter</d:Types>" +
                    $"<d:Scopes>{Esc(string.Join(' ', ScopeValues()))}</d:Scopes><d:XAddrs>{_config.DeviceUrl}</d:XAddrs><d:MetadataVersion>1</d:MetadataVersion>" +
                    $"</d:{match}></d:{action}></s:Body></s:Envelope>";
                await udp.SendAsync(Encoding.UTF8.GetBytes(response), request.RemoteEndPoint, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (SocketException)
        {
            // Manual Advanced Adoption still works when multicast is blocked or this address is not locally assigned.
            Log.Warn("ONVIF discovery unavailable; use the advertised camera IP and ONVIF port for manual adoption");
        }
    }
}
