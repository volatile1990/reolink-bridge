// Reolink Bridge authenticated ONVIF PullPoint event service; AGPL-3.0.
using System.Globalization;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;

namespace Neolink.Onvif;

public sealed partial class ProtectOnvifServer
{
    private const string SubscriptionPrefix = "/onvif/subscriptions/";
    private const string ConcreteSet = "http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet";
    private const string Concrete = "http://docs.oasis-open.org/wsn/t-1/TopicExpression/Concrete";
    private string EventsUrl => $"http://{_config.AdvertisedHost}:{_config.Port}/onvif/events_service";
    private string SubscriptionUrl(string id) => $"http://{_config.AdvertisedHost}:{_config.Port}{SubscriptionPrefix}{id}";
    private static bool ValidSubscriptionPath(string path) => path.StartsWith(SubscriptionPrefix, StringComparison.Ordinal)
        && Guid.TryParseExact(path[SubscriptionPrefix.Length..], "N", out _);

    private async Task EventReplayAsync(NetworkStream connection, string target, string? authorization, CancellationToken ct)
    {
        if (!AuthenticatedBasic(authorization) && !_snapshotDigest.Authenticate(authorization, "GET", target))
        {
            await RespondAsync(connection, 401, "{\"error\":\"authentication-required\"}", true, ct,
                "application/json", _snapshotDigest.Challenge()).ConfigureAwait(false);
            return;
        }
        if (Events == null)
        {
            await RespondAsync(connection, 404, "{\"error\":\"events-disabled\"}", true, ct, "application/json").ConfigureAwait(false);
            return;
        }
        long after = 0;
        int limit = 128;
        bool valid = true;
        var seen = new HashSet<string>(StringComparer.Ordinal);
        int question = target.IndexOf('?');
        if (question >= 0)
            foreach (string part in target[(question + 1)..].Split('&'))
            {
                int equals = part.IndexOf('=');
                if (equals < 1) { valid = false; break; }
                string name = part[..equals], value = part[(equals + 1)..];
                if (!seen.Add(name)) { valid = false; break; }
                if (name == "after") valid &= long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out after) && after >= 0;
                else if (name == "limit") valid &= int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out limit) && limit is >= 1 and <= ProtectEventBroker.MaxQueue;
                else valid = false;
            }
        await RespondAsync(connection, valid ? 200 : 400,
            valid ? JsonSerializer.Serialize(Events.Replay(after, limit)) : "{\"error\":\"invalid-event-cursor\"}",
            true, ct, "application/json").ConfigureAwait(false);
    }

    // Only called after authentication has succeeded; do not authenticate again and consume a WSSE nonce twice.
    private string? AuthenticatedOwner(XElement envelope, string? authorization)
    {
        if (_users.Count == 0) return null;
        if (authorization != null) return NetUtil.DecodeBasicAuth(authorization)?.User;
        return envelope.Elements().FirstOrDefault(e => e.Name.LocalName == "Header")?
            .Descendants().FirstOrDefault(e => e.Name.LocalName == "UsernameToken")?
            .Elements().FirstOrDefault(e => e.Name.LocalName == "Username")?.Value;
    }

    private async Task<(int Status, string Body)> HandleEventAsync(XElement envelope, XElement op, string owner,
        string? requestPath, CancellationToken ct)
    {
        if (Events == null) return Fault(400, "ter:ActionNotSupported", "Event service is disabled");
        string action = op.Name.LocalName, ns = op.Name.NamespaceName;
        bool manager = action is "PullMessages" or "SetSynchronizationPoint" or "Renew" or "Unsubscribe";
        string? path = requestPath;
        // Direct HandleAsync callers and clients using WS-Addressing may supply the advertised endpoint in To.
        if (path == null)
        {
            string? to = envelope.Elements().FirstOrDefault(e => e.Name.LocalName == "Header")?.Element(XName.Get("To", NsWsa))?.Value;
            if (Uri.TryCreate(to, UriKind.Absolute, out var uri) && uri.Host == _config.AdvertisedHost && uri.Port == _config.Port)
                path = uri.AbsolutePath;
        }
        if (manager && (path == null || !ValidSubscriptionPath(path)))
            return Fault(400, "wsnt:ResourceUnknown", "Use the advertised subscription endpoint");
        if (!manager && path != null && path != "/onvif/events_service")
            return Fault(400, "ter:ActionNotSupported", "Use the event service endpoint");
        try
        {
            string body, responseAction;
            if (ns == NsEvents)
                switch (action)
                {
                    case "GetServiceCapabilities":
                        body = $"<tev:GetServiceCapabilitiesResponse><tev:Capabilities WSSubscriptionPolicySupport=\"false\" WSPullPointSupport=\"true\" WSPausableSubscriptionManagerInterfaceSupport=\"false\" MaxPullPoints=\"{ProtectEventBroker.MaxSubscriptions}\" PersistentNotificationStorage=\"false\"/></tev:GetServiceCapabilitiesResponse>";
                        responseAction = NsEvents + "/EventPortType/GetServiceCapabilitiesResponse";
                        break;
                    case "GetEventProperties":
                        body = EventProperties();
                        responseAction = NsEvents + "/EventPortType/GetEventPropertiesResponse";
                        break;
                    case "CreatePullPointSubscription":
                        var sub = Events.Subscribe(owner, Lease(Child(op, "InitialTerminationTime")), TopicFilter(op));
                        body = "<tev:CreatePullPointSubscriptionResponse><tev:SubscriptionReference>" +
                            $"<wsa:Address>{Esc(SubscriptionUrl(sub.Id))}</wsa:Address></tev:SubscriptionReference>" +
                            $"<wsnt:CurrentTime>{Utc(sub.CurrentTime)}</wsnt:CurrentTime><wsnt:TerminationTime>{Utc(sub.TerminationTime)}</wsnt:TerminationTime>" +
                            "</tev:CreatePullPointSubscriptionResponse>";
                        responseAction = NsEvents + "/EventPortType/CreatePullPointSubscriptionResponse";
                        break;
                    case "PullMessages":
                        TimeSpan wait = Duration(Child(op, "Timeout") ?? "PT2S");
                        if (!int.TryParse(Child(op, "MessageLimit") ?? "10", NumberStyles.None, CultureInfo.InvariantCulture, out int limit))
                            throw new ProtectEventFault("InvalidArgVal", "Invalid message limit");
                        var batch = await Events.PullAsync(path![SubscriptionPrefix.Length..], owner, wait, limit, ct).ConfigureAwait(false);
                        body = $"<tev:PullMessagesResponse><tev:CurrentTime>{Utc(batch.CurrentTime)}</tev:CurrentTime>" +
                            $"<tev:TerminationTime>{Utc(batch.TerminationTime)}</tev:TerminationTime>" + string.Concat(batch.Events.Select(Notification)) + "</tev:PullMessagesResponse>";
                        responseAction = NsEvents + "/PullPointSubscription/PullMessagesResponse";
                        break;
                    case "SetSynchronizationPoint":
                        Events.Synchronize(path![SubscriptionPrefix.Length..], owner);
                        body = "<tev:SetSynchronizationPointResponse/>";
                        responseAction = NsEvents + "/PullPointSubscription/SetSynchronizationPointResponse";
                        break;
                    default: return Fault(400, "ter:ActionNotSupported", "Unsupported event operation");
                }
            else if (ns == NsWsnt && action == "Renew")
            {
                var sub = Events.Renew(path![SubscriptionPrefix.Length..], owner, Lease(Child(op, "TerminationTime")));
                body = $"<wsnt:RenewResponse><wsnt:TerminationTime>{Utc(sub.TerminationTime)}</wsnt:TerminationTime>" +
                    $"<wsnt:CurrentTime>{Utc(sub.CurrentTime)}</wsnt:CurrentTime></wsnt:RenewResponse>";
                responseAction = "http://docs.oasis-open.org/wsn/bw-2/SubscriptionManager/RenewResponse";
            }
            else if (ns == NsWsnt && action == "Unsubscribe")
            {
                Events.Unsubscribe(path![SubscriptionPrefix.Length..], owner);
                body = "<wsnt:UnsubscribeResponse/>";
                responseAction = "http://docs.oasis-open.org/wsn/bw-2/SubscriptionManager/UnsubscribeResponse";
            }
            else return Fault(400, "ter:ActionNotSupported", "Unsupported subscription operation");
            var response = XDocument.Parse(Envelope(body));
            string? messageId = envelope.Elements().FirstOrDefault(e => e.Name.LocalName == "Header")?.Element(XName.Get("MessageID", NsWsa))?.Value;
            var header = new XElement(XName.Get("Header", NsSoap),
                new XElement(XName.Get("Action", NsWsa), responseAction));
            if (messageId is { Length: > 0 and <= 256 }) header.Add(new XElement(XName.Get("RelatesTo", NsWsa), messageId));
            response.Root!.AddFirst(header);
            return (200, response.ToString(SaveOptions.DisableFormatting));
        }
        catch (ProtectEventFault ex)
        {
            string prefix = ex.Code is "ResourceUnknown" or "UnacceptableTerminationTime" ? "wsnt:" : "ter:";
            return Fault(400, prefix + ex.Code, ex.Message);
        }
    }

    private static string Utc(DateTimeOffset value) => value.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture);
    private static TimeSpan Duration(string text)
    {
        try { return XmlConvert.ToTimeSpan(text); }
        catch (Exception ex) when (ex is FormatException or OverflowException)
        { throw new ProtectEventFault("InvalidArgVal", "Invalid duration"); }
    }
    private static TimeSpan Lease(string? text)
    {
        if (text == null) return TimeSpan.FromMinutes(5);
        if (text.StartsWith('P')) return Duration(text);
        if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var at))
            return at - DateTimeOffset.UtcNow;
        throw new ProtectEventFault("UnacceptableTerminationTime", "Invalid lease");
    }
    private static Func<string, bool>? TopicFilter(XElement operation)
    {
        var filter = operation.Elements().FirstOrDefault(e => e.Name.LocalName == "Filter");
        if (filter == null) return null;
        if (filter.Elements().Count() != 1 || filter.Elements().Single().Name != XName.Get("TopicExpression", NsWsnt))
            throw new ProtectEventFault("InvalidFilter", "Only a topic filter is supported");
        var expression = filter.Elements().Single();
        if (expression.Attribute("Dialect")?.Value is not (ConcreteSet or Concrete))
            throw new ProtectEventFault("TopicExpressionDialectUnknown", "Unsupported topic dialect");
        if (expression.Value.Length > 2048) throw new ProtectEventFault("InvalidFilter", "Topic filter is too long");
        string[] choices = expression.Value.Split('|').Select(s => s.Trim()).ToArray();
        if (choices.Length is < 1 or > 16) throw new ProtectEventFault("InvalidFilter", "Too many topics");
        var patterns = choices.Select(choice =>
        {
            if (!Regex.IsMatch(choice, "^[A-Za-z_][A-Za-z0-9_:./]*$", RegexOptions.CultureInvariant))
                throw new ProtectEventFault("InvalidFilter", "Invalid topic expression");
            return string.Join('/', choice.Split('/').Select(segment =>
            {
                int colon = segment.IndexOf(':');
                if (colon < 0) return segment;
                string prefix = segment[..colon];
                string local = segment[(colon + 1)..];
                string? ns = expression.GetNamespaceOfPrefix(prefix)?.NamespaceName;
                return ns switch
                {
                    NsTopics => "tns1:" + local,
                    NsAi => "tnsre:" + local,
                    _ => throw new ProtectEventFault("InvalidFilter", "Unknown topic namespace")
                };
            }));
        }).ToArray();
        return topic => patterns.Any(pattern => pattern.EndsWith("//.", StringComparison.Ordinal)
            ? topic.StartsWith(pattern[..^3] + "/", StringComparison.Ordinal) : topic == pattern);
    }
    private string EventProperties()
    {
        const string source = "<tt:Source><tt:SimpleItemDescription Name=\"VideoSourceConfigurationToken\" Type=\"tt:ReferenceToken\"/></tt:Source>";
        string ai = string.Concat(Events!.ObservedClasses.Select(kind =>
            $"<tnsre:{char.ToUpperInvariant(kind[0]) + kind[1..]} wstop:topic=\"true\"><tt:MessageDescription IsProperty=\"true\">{source}" +
            "<tt:Data><tt:SimpleItemDescription Name=\"State\" Type=\"xs:boolean\"/><tt:SimpleItemDescription Name=\"ObjectType\" Type=\"xs:string\"/></tt:Data>" +
            $"</tt:MessageDescription></tnsre:{char.ToUpperInvariant(kind[0]) + kind[1..]}>"));
        return "<tev:GetEventPropertiesResponse><tev:TopicNamespaceLocation>http://www.onvif.org/onvif/ver10/topics/topicns.xml</tev:TopicNamespaceLocation>" +
            "<tev:FixedTopicSet>false</tev:FixedTopicSet><wstop:TopicSet><tns1:RuleEngine wstop:topic=\"true\">" +
            "<CellMotionDetector wstop:topic=\"true\"><Motion wstop:topic=\"true\"><tt:MessageDescription IsProperty=\"true\">" + source +
            "<tt:Data><tt:SimpleItemDescription Name=\"IsMotion\" Type=\"xs:boolean\"/></tt:Data></tt:MessageDescription></Motion></CellMotionDetector>" +
            (ai.Length == 0 ? "" : "<tnsre:ReolinkAI wstop:topic=\"true\">" + ai + "</tnsre:ReolinkAI>") +
            $"</tns1:RuleEngine></wstop:TopicSet><tev:TopicExpressionDialect>{ConcreteSet}</tev:TopicExpressionDialect>" +
            $"<tev:TopicExpressionDialect>{Concrete}</tev:TopicExpressionDialect>" +
            "<tev:MessageContentSchemaLocation>http://www.onvif.org/ver10/schema/onvif.xsd</tev:MessageContentSchemaLocation></tev:GetEventPropertiesResponse>";
    }
    private string Notification(ProtectCameraEvent ev)
    {
        string data = ev.Kind == "motion" ? $"<tt:SimpleItem Name=\"IsMotion\" Value=\"{(ev.Active ? "true" : "false")}\"/>"
            : $"<tt:SimpleItem Name=\"State\" Value=\"{(ev.Active ? "true" : "false")}\"/><tt:SimpleItem Name=\"ObjectType\" Value=\"{ev.Kind}\"/>";
        return $"<wsnt:NotificationMessage><wsnt:Topic Dialect=\"{Concrete}\">{ProtectEventBroker.Topic(ev.Kind)}</wsnt:Topic>" +
            $"<wsnt:Message><tt:Message UtcTime=\"{Utc(ev.At)}\" PropertyOperation=\"{ev.Operation}\"><tt:Source>" +
            $"<tt:SimpleItem Name=\"VideoSourceConfigurationToken\" Value=\"source_config_{Esc(_streams[0].Token)}\"/></tt:Source>" +
            $"<tt:Data>{data}</tt:Data></tt:Message></wsnt:Message></wsnt:NotificationMessage>";
    }
}
