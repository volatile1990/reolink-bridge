using System.Net;
using System.Net.Sockets;
using System.Security;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Neolink.Config;
using Neolink.Onvif;
using Neolink.Protocol;

internal static class ProtectEventContractTests
{
    private const string User = "events-test", Password = "synthetic-event-only<&secret";
    private const string SetDialect = "http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet";
    private const string EventPath = "/onvif/events_service";
    private static void Check(bool ok, string message) { if (!ok) throw new Exception(message); }
    private static string Esc(string text) => SecurityElement.Escape(text)!;
    private static XElement Node(XElement xml, string name) => xml.Descendants().First(e => e.Name.LocalName == name);
    private static string Address(Reply reply) => Node(reply.Xml, "Address").Value;
    private static string Path(Reply reply) => new Uri(Address(reply)).AbsolutePath;

    public static async Task DiscoveryAndClasses()
    {
        await using var f = await Fixture.Start();
        var services = await f.Call("GetServices", ns: ProtectOnvifServer.NsDevice, path: "/onvif/device_service");
        Check(services.Status == 200 && services.Body.Contains(ProtectOnvifServer.NsEvents), "events are not discoverable");
        var cap = await f.Call("GetCapabilities", ns: ProtectOnvifServer.NsDevice, path: "/onvif/device_service");
        Check(Node(cap.Xml, "WSPullPointSupport").Value == "true", "PullPoint capability missing");
        var detail = await f.Call("GetServiceCapabilities");
        Check(Node(detail.Xml, "Capabilities").Attribute("MaxPullPoints")?.Value == "8", "wrong resource limit");
        var properties = await f.Call("GetEventProperties");
        Check(properties.Status == 200 && properties.Body.Contains("IsMotion"), "motion description missing");
        Check(!properties.Body.Contains("ObjectType") && !properties.Body.Contains("tnsre:Person"), "unobserved class advertised");
        f.Broker.Publish(new MotionPush("MD", ["people"]));
        properties = await f.Call("GetEventProperties");
        Check(properties.Body.Contains("tnsre:Person") && !properties.Body.Contains("tnsre:Vehicle")
            && !properties.Body.Contains("tnsre:Animal"), "properties invented a class");
        var metrics = await f.Http.GetAsync("/metrics");
        // GET has separate Basic auth; an anonymous client may not read observations.
        Check(metrics.StatusCode == HttpStatusCode.Unauthorized, "event diagnostics became public");
        using var request = new HttpRequestMessage(HttpMethod.Get, "/metrics");
        request.Headers.TryAddWithoutValidation("Authorization", f.Basic);
        using var read = await f.Http.SendAsync(request);
        string json = await read.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var observed = doc.RootElement.GetProperty("events").GetProperty("observedClasses");
        Check(observed.GetArrayLength() == 1 && observed[0].GetString() == "person", "observations missing from diagnostics");
        Check(!json.Contains(User) && !json.Contains(Password), "diagnostics leaked credentials");
    }

    public static async Task CameraPushToWire()
    {
        await using var f = await Fixture.Start();
        var created = await f.Call("CreatePullPointSubscription", "<e:InitialTerminationTime>PT300S</e:InitialTerminationTime>");
        string path = Path(created);
        Check(!Address(created).Contains(User) && !Address(created).Contains(Password), "subscription URI has credentials");
        var initial = await f.Pull(path);
        Check(initial.Notifications.Length == 1 && Node(initial.Xml, "Message").Name.NamespaceName == ProtectOnvifServer.NsWsnt,
            "initial property has wrong notification wrapper");
        var state = initial.Xml.Descendants(XName.Get("Message", ProtectOnvifServer.NsSchema)).Single();
        Check(state.Attribute("PropertyOperation")?.Value == "Initialized", "initial state not marked initialized");
        Check(Data(initial, "IsMotion").Single() == "false", "cold camera invented motion");
        // Parse the actual Baichuan wire dialect, including repeated/comma AItype values and nested AI tokens.
        var raw = XElement.Parse("<AlarmEvent><channelId>0</channelId><status>MD</status>" +
            "<AItype>people,vehicle</AItype><AItype>DOG_CAT;none</AItype></AlarmEvent>");
        f.Broker.Publish(BcCamera.ParseAlarmEvent(raw));
        var active = await f.Pull(path);
        Check(active.Notifications.Length == 4 && Data(active, "IsMotion").Single() == "true", "camera push lost motion or classes");
        Check(Data(active, "ObjectType").Order().SequenceEqual(new[] { "animal", "person", "vehicle" }), "wrong real classes");
        var motion = active.Notifications.Single(n => Node(n, "Topic").Value.Contains("CellMotionDetector"));
        Check(Node(motion, "Topic").Value == ProtectEventBroker.MotionTopic, "Protect-compatible motion topic changed");
        var source = motion.Descendants(XName.Get("SimpleItem", ProtectOnvifServer.NsSchema))
            .Single(n => n.Attribute("Name")?.Value == "VideoSourceConfigurationToken");
        Check(source.Attribute("Value")?.Value == "source_config_main", "source token differs from media profile");
        f.Broker.Publish(BcCamera.ParseAlarmEvent(raw));
        Check((await f.Pull(path)).Notifications.Length == 0, "duplicate active alarm emitted duplicate state transitions");
        f.Broker.Publish(BcCamera.ParseAlarmEvent(XElement.Parse("<AlarmEvent><status>none</status><AItype>none</AItype></AlarmEvent>")));
        var stop = await f.Pull(path);
        Check(stop.Notifications.Length == 4 && Data(stop, "IsMotion").Single() == "false"
            && Data(stop, "State").All(v => v == "false"), "all-clear left a class active");
        // A real AI-only Baichuan push with status none remains an actual detection.
        f.Broker.Publish(BcCamera.ParseAlarmEvent(XElement.Parse("<AlarmEvent><status>none</status><AItype>none</AItype>" +
            "<smartAiTypeList><smartAiType><type>intrusion</type><subList><type>people</type></subList></smartAiType></smartAiTypeList></AlarmEvent>")));
        var smart = await f.Pull(path);
        Check(Data(smart, "IsMotion").Single() == "true" && Data(smart, "ObjectType").Single() == "person", "nested camera verdict lost");
        f.Broker.Publish(new MotionPush("MD", ["vehicle"], External: true));
        Check((await f.Pull(path)).Notifications.Length == 0, "outside control was misrepresented as a camera detection");
    }

    public static async Task AuthenticationAndOwnership()
    {
        await using var f = await Fixture.Start();
        foreach (string action in new[] { "GetEventProperties", "GetServiceCapabilities", "CreatePullPointSubscription" })
            Check((await f.Call(action, authenticate: false)).Status == 401, "anonymous event request succeeded");
        var created = await f.Call("CreatePullPointSubscription");
        string path = Path(created);
        foreach (string action in new[] { "PullMessages", "SetSynchronizationPoint", "Renew", "Unsubscribe" })
        {
            string ns = action is "Renew" or "Unsubscribe" ? ProtectOnvifServer.NsWsnt : ProtectOnvifServer.NsEvents;
            Check((await f.Call(action, path: path, ns: ns, authenticate: false)).Status == 401, "anonymous manager request succeeded");
            Check((await f.Call(action, path: path, ns: ns, user: "events-other")).Status == 400, "other owner accessed a subscription");
        }
        Check((await f.Pull(path)).Notifications.Length == 1, "rejected owner consumed the original owner's event");
        string token = Fixture.Token(User);
        Check((await f.Call("GetEventProperties", security: token)).Status == 200, "valid WSSE digest was rejected");
        Check((await f.Call("GetEventProperties", security: token)).Status == 401, "replayed WSSE token was accepted");
        var basic = await f.Call("GetEventProperties", authorization: f.Basic);
        Check(basic.Status == 200, "Basic event authentication stopped working");
        var wrong = await f.Call("GetEventProperties", authorization: "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(User + ":wrong")));
        Check(wrong.Status == 401 && !wrong.Body.Contains("wrong") && !wrong.Body.Contains(User), "wrong authentication leaked or succeeded");
    }

    public static async Task LifecycleAndFilters()
    {
        await using var f = await Fixture.Start();
        f.Broker.Publish(new MotionPush("MD", ["people", "vehicle"]));
        string filter = $"<e:Filter><wsnt:TopicExpression xmlns:wsnt=\"{ProtectOnvifServer.NsWsnt}\" xmlns:cam=\"{ProtectOnvifServer.NsTopics}\" Dialect=\"{SetDialect}\">cam:RuleEngine/CellMotionDetector/Motion</wsnt:TopicExpression></e:Filter>";
        var created = await f.Call("CreatePullPointSubscription", filter);
        Check(created.Status == 200, "valid motion filter refused");
        string path = Path(created);
        var first = await f.Pull(path);
        Check(first.Notifications.Length == 1 && Data(first, "IsMotion").Single() == "true", "filter leaked AI topics");
        var sync = await f.Call("SetSynchronizationPoint", path: path);
        Check(sync.Status == 200 && Node((await f.Pull(path)).Xml, "Message").Name.NamespaceName == ProtectOnvifServer.NsWsnt,
            "synchronization did not initialize filtered state");
        var renew = await f.Call("Renew", "<e:TerminationTime>PT600S</e:TerminationTime>", ProtectOnvifServer.NsWsnt, path);
        Check(renew.Status == 200 && DateTimeOffset.Parse(Node(renew.Xml, "TerminationTime").Value)
            - DateTimeOffset.Parse(Node(renew.Xml, "CurrentTime").Value) > TimeSpan.FromSeconds(599), "renew did not grant a lease");
        Check((await f.Call("Unsubscribe", ns: ProtectOnvifServer.NsWsnt, path: path)).Status == 200, "unsubscribe failed");
        Check((await f.Pull(path)).Status == 400, "dead subscription remained accessible");
        Check((await f.Call("PullMessages")).Status == 400, "service endpoint masqueraded as a subscription");
        Check((await f.Call("CreatePullPointSubscription", "<e:InitialTerminationTime>PT3601S</e:InitialTerminationTime>")).Status == 400, "unbounded lease accepted");
        Check((await f.Call("CreatePullPointSubscription", filter.Replace(SetDialect, "urn:unsupported"))).Status == 400, "unsupported filter silently ignored");
        Check((await f.Call("CreatePullPointSubscription", "<e:Filter><e:Unknown/></e:Filter>")).Status == 400, "unsupported filter payload accepted");
        var wildcard = await f.Call("CreatePullPointSubscription", filter.Replace("cam:RuleEngine/CellMotionDetector/Motion", "cam:RuleEngine//."));
        Check((await f.Pull(Path(wildcard))).Notifications.Length == 3, "ConcreteSet subtree filter lost state");
        Check((await f.Call("PullMessages", "<e:Timeout>PT31S</e:Timeout><e:MessageLimit>1</e:MessageLimit>", path: Path(wildcard))).Status == 400, "oversized poll accepted");
        Check((await f.Call("PullMessages", "<e:Timeout>PT0S</e:Timeout><e:MessageLimit>65</e:MessageLimit>", path: Path(wildcard))).Status == 400, "oversized batch accepted");
    }

    public static async Task LongPollAndCancellation()
    {
        var broker = new ProtectEventBroker();
        var sub = broker.Subscribe(User, TimeSpan.FromMinutes(5));
        await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        using var stop = new CancellationTokenSource();
        Task<ProtectEventBatch> wait = broker.PullAsync(sub.Id, User, TimeSpan.FromSeconds(2), 10, stop.Token);
        bool overlap = false;
        try { await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None); }
        catch (ProtectEventFault) { overlap = true; }
        Check(overlap && !wait.IsCompleted, "long poll didn't wait or overlap wasn't rejected");
        broker.Publish(new MotionPush("MD", []));
        Check((await wait.WaitAsync(TimeSpan.FromSeconds(1))).Events.Single().Active, "camera push didn't wake poll");
        wait = broker.PullAsync(sub.Id, User, TimeSpan.FromSeconds(2), 10, stop.Token);
        stop.Cancel();
        bool canceled = false;
        try { await wait; } catch (OperationCanceledException) { canceled = true; }
        Check(canceled, "poll ignored cancellation");
        broker.Publish(new MotionPush("none", []));
        Check(!(await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None)).Events.Single().Active,
            "cancelled poll consumed the later all-clear");
        wait = broker.PullAsync(sub.Id, User, TimeSpan.FromSeconds(2), 10, CancellationToken.None);
        broker.Unsubscribe(sub.Id, User);
        bool lost = false;
        try { await wait.WaitAsync(TimeSpan.FromSeconds(1)); } catch (ProtectEventFault) { lost = true; }
        Check(lost, "unsubscribe did not release a pending poll");
        // Exercise a real HTTP long poll with Protect's actual 2 s / 10 message request shape.
        await using var f = await Fixture.Start();
        var created = await f.Call("CreatePullPointSubscription");
        await f.Pull(Path(created));
        Task<Reply> wire = f.Call("PullMessages", "<e:Timeout>PT2S</e:Timeout><e:MessageLimit>10</e:MessageLimit>", path: Path(created));
        await Task.Delay(50);
        f.Broker.Publish(new MotionPush("MD", []));
        Check(Data(await wire.WaitAsync(TimeSpan.FromSeconds(1)), "IsMotion").Single() == "true", "wire poll missed the event");
    }

    public static async Task LeasesAndStaleState()
    {
        var clock = new ManualTimeProvider();
        var broker = new ProtectEventBroker(5, clock);
        var sub = broker.Subscribe(User, TimeSpan.FromSeconds(10));
        broker.Publish(new MotionPush("MD", ["people"]));
        await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        clock.WallClock = DateTimeOffset.UtcNow.AddYears(5);
        clock.Advance(TimeSpan.FromSeconds(6));
        broker.Tick();
        var clears = await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        Check(clears.Events.Count == 2 && clears.Events.All(e => !e.Active), "stale alarms didn't clear");
        Check(broker.ObservedClasses.SequenceEqual(new[] { "person" }), "stale clear erased actual camera capabilities");
        clock.Advance(TimeSpan.FromSeconds(5));
        bool expired = false;
        try { await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None); }
        catch (ProtectEventFault) { expired = true; }
        Check(expired, "wall-clock change prevented monotonic lease expiry");
        sub = broker.Subscribe(User, TimeSpan.FromMinutes(1));
        await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        broker.Publish(new MotionPush("MD", ["animal"]));
        await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        broker.ResetActive();
        Check((await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None)).Events.All(e => !e.Active), "source-loss clear failed");
        var generationBroker = new ProtectEventBroker();
        generationBroker.Publish(new MotionPush("MD", ["people"]));
        generationBroker.ResetActive();
        generationBroker.Publish(new MotionPush("MD", ["people"]));
        using var generations = JsonDocument.Parse(JsonSerializer.Serialize(generationBroker.Replay(0)));
        var real = generations.RootElement.GetProperty("events");
        Check(real.GetArrayLength() == 2 && generations.RootElement.GetProperty("currentSeq").GetInt64() == 2
            && real[0].GetProperty("source").GetProperty("connectionEpoch").GetInt64()
                < real[1].GetProperty("source").GetProperty("connectionEpoch").GetInt64(),
            "source reset inserted a fake camera push or failed to distinguish the next genuine class pulse");
    }

    public static async Task BoundsAndOverflow()
    {
        var broker = new ProtectEventBroker();
        var subs = Enumerable.Range(0, ProtectEventBroker.MaxSubscriptions).Select(_ => broker.Subscribe(User, TimeSpan.FromMinutes(1))).ToArray();
        bool full = false;
        try { broker.Subscribe(User, TimeSpan.FromMinutes(1)); } catch (ProtectEventFault) { full = true; }
        Check(full, "unbounded pull points were allocated");
        for (int i = 0; i < 600; i++) broker.Publish(new MotionPush(i % 2 == 0 ? "MD" : "none", i % 2 == 0 ? new[] { "people", "vehicle", "dog_cat" } : Array.Empty<string>()));
        using var stats = JsonDocument.Parse(JsonSerializer.Serialize(broker.Diagnostics()));
        Check(stats.RootElement.GetProperty("queuedMessages").GetInt32() <= ProtectEventBroker.MaxSubscriptions * ProtectEventBroker.MaxQueue
            && stats.RootElement.GetProperty("queueResets").GetInt64() > 0, "event queues aren't bounded or overflow wasn't tracked");
        var latest = new Dictionary<string, bool>();
        while (true)
        {
            var batch = await broker.PullAsync(subs[0].Id, User, TimeSpan.Zero, 64, CancellationToken.None);
            if (batch.Events.Count == 0) break;
            foreach (var ev in batch.Events) latest[ev.Kind] = ev.Active;
        }
        Check(latest.Count == 4 && latest.Values.All(active => !active), "queue overflow left a detection stuck on");
        broker.Stop();
        using var stopped = JsonDocument.Parse(JsonSerializer.Serialize(broker.Diagnostics()));
        Check(stopped.RootElement.GetProperty("subscriptions").GetInt32() == 0, "stop left subscriptions allocated");
    }

    public static async Task DisabledEvents()
    {
        await using var f = await Fixture.Start(enabled: false);
        Check(f.Server.Events == null, "disabled events allocated a broker");
        var services = await f.Call("GetServices", ns: ProtectOnvifServer.NsDevice, path: "/onvif/device_service");
        Check(!services.Body.Contains(ProtectOnvifServer.NsEvents + "</tds:Namespace>"), "disabled event service advertised");
        Check((await f.Call("GetEventProperties")).Status == 400, "disabled event service answered");
    }

    public static Task MotionPolicyConfiguration()
    {
        const string json = """{"advertised_host":"127.0.0.1","mac":"02:54:45:53:55:01","uuid":"c32a2a5c-59f6-48d0-9301-ed09d3a40313","profiles":[{"width":1920,"height":1080}]}""";
        using var baseline = JsonDocument.Parse(json);
        Check(BridgeOnvifConfig.Parse(baseline.RootElement).MotionEventPolicy == "all", "existing configuration stopped forwarding all movement");
        foreach (string policy in new[] { "all", "classified", "none" })
        {
            using var document = JsonDocument.Parse(json[..^1] + ",\"motion_event_policy\":\"" + policy + "\"}");
            Check(BridgeOnvifConfig.Parse(document.RootElement).MotionEventPolicy == policy, "motion policy was not parsed");
        }
        using var wrong = JsonDocument.Parse(json[..^1] + ",\"motion_event_policy\":\"classifed\"}");
        bool rejected = false;
        try { BridgeOnvifConfig.Parse(wrong.RootElement); } catch (FormatException) { rejected = true; }
        Check(rejected, "misspelled policy silently enabled a different behavior");
        rejected = false;
        try { _ = new ProtectEventBroker(motionEventPolicy: "unknown"); } catch (ArgumentOutOfRangeException) { rejected = true; }
        Check(rejected, "broker accepted unknown policy");
        var broker = new ProtectEventBroker();
        broker.Publish(new MotionPush("MD", []));
        using var metrics = JsonDocument.Parse(JsonSerializer.Serialize(broker.Diagnostics()));
        Check(metrics.RootElement.GetProperty("motionEventPolicy").GetString() == "all"
            && metrics.RootElement.GetProperty("forwardedMotionStarts").GetInt64() == 1
            && metrics.RootElement.GetProperty("suppressedUnclassifiedPushes").GetInt64() == 0,
            "default motion forwarding or default metrics changed");
        return Task.CompletedTask;
    }

    public static async Task ClassifiedMotionWireAndReplay()
    {
        await using var f = await Fixture.Start(motionPolicy: "classified");
        var created = await f.Call("CreatePullPointSubscription");
        string path = Path(created);
        Check(Data(await f.Pull(path), "IsMotion").Single() == "false", "cold classified gate invented motion");
        // Generic motion may remain active for many packets before a real class arrives.
        f.Broker.Publish(new MotionPush("MD", []));
        f.Broker.Publish(new MotionPush("MD", []));
        f.Broker.Publish(new MotionPush("unknown-status", []));
        f.Broker.Publish(new MotionPush("false", []));
        f.Broker.Publish(new MotionPush("MD", ["none", "motion", "MD", "false", "true", "0", "1", " \t"]));
        Check((await f.Pull(path)).Notifications.Length == 0, "generic motion or placeholder AItype opened classified gate");
        var parsed = BcCamera.ParseAlarmEvent(XElement.Parse("<AlarmEvent><status>MD</status><AItype>people,vehicle,dog_cat</AItype></AlarmEvent>"));
        f.Broker.Publish(parsed);
        var active = await f.Pull(path);
        Check(active.Notifications.Length == 4 && Data(active, "IsMotion").Single() == "true"
            && Data(active, "ObjectType").Order().SequenceEqual(new[] { "animal", "person", "vehicle" }),
            "class arrival during long generic motion was lost or native class notification changed");
        f.Broker.Publish(parsed);
        Check((await f.Pull(path)).Notifications.Length == 0, "repeated class created extra rises");
        f.Broker.Publish(new MotionPush("MD", []));
        var cleared = await f.Pull(path);
        Check(cleared.Notifications.Length == 4 && Data(cleared, "IsMotion").Single() == "false"
            && Data(cleared, "State").All(value => value == "false"), "continued plant motion kept the classified gate active");
        // An unfamiliar real camera verdict must count without inventing a known class or topic.
        f.Broker.Publish(BcCamera.ParseAlarmEvent(XElement.Parse("<AlarmEvent><status>none</status><AItype>unfamiliar-ai-type</AItype></AlarmEvent>")));
        var unknown = await f.Pull(path);
        Check(unknown.Notifications.Length == 1 && Data(unknown, "IsMotion").Single() == "true"
            && !Data(unknown, "ObjectType").Any(), "unknown real AI verdict was dropped or relabeled");
        f.Broker.Publish(new MotionPush("none", []));
        Check(Data(await f.Pull(path), "IsMotion").Single() == "false", "all-clear left classified motion active");
        f.Broker.Publish(new MotionPush("MD", ["people"], External: true));
        Check((await f.Pull(path)).Notifications.Length == 0, "external control bypassed the classified gate");
        using var replay = await Replay(f, "/events?after=0&limit=128");
        var rows = replay.RootElement.GetProperty("events");
        Check(rows.GetArrayLength() == 10 && rows[0].GetProperty("motion").GetBoolean()
            && rows[0].GetProperty("isActive").GetBoolean() && rows[0].GetProperty("rawAiTypes").GetArrayLength() == 0,
            "gating removed generic raw pushes or rewrote source activity");
        Check(rows[8].GetProperty("rawAiTypes")[0].GetString() == "unfamiliar-ai-type"
            && rows[8].GetProperty("classes").GetArrayLength() == 0, "unknown raw verdict was lost or falsely classified");
        using var metrics = JsonDocument.Parse(JsonSerializer.Serialize(f.Broker.Diagnostics()));
        Check(metrics.RootElement.GetProperty("motionEventPolicy").GetString() == "classified"
            && metrics.RootElement.GetProperty("suppressedUnclassifiedPushes").GetInt64() == 6
            && metrics.RootElement.GetProperty("forwardedMotionStarts").GetInt64() == 2
            && metrics.RootElement.GetProperty("forwardedMotionClears").GetInt64() == 2,
            "motion policy evidence counters are wrong");
    }

    public static async Task ClassifiedMotionStaleResetAndSynchronization()
    {
        var clock = new ManualTimeProvider();
        var broker = new ProtectEventBroker(5, clock, motionEventPolicy: "classified");
        var sub = broker.Subscribe(User, TimeSpan.FromMinutes(5));
        await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        broker.Publish(new MotionPush("MD", []));
        broker.Synchronize(sub.Id, User);
        Check(!(await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None)).Events.Single().Active,
            "synchronization exposed suppressed raw movement");
        broker.Publish(new MotionPush("none", ["people"]));
        var active = await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        Check(active.Events.Count == 2 && active.Events.All(value => value.Active), "AI-only source did not open motion and class");
        clock.Advance(TimeSpan.FromSeconds(6));
        broker.Tick();
        var stale = await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        Check(stale.Events.Count == 2 && stale.Events.All(value => !value.Active), "stale classified motion did not clear");
        broker.Publish(new MotionPush("MD", ["vehicle"]));
        await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        broker.ResetActive();
        Check((await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None)).Events.All(value => !value.Active),
            "connection loss failed to clear classified motion");
        broker.Publish(new MotionPush("MD", []));
        Check((await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None)).Events.Count == 0,
            "generic movement reopened classified gate after reconnect");
        using var replay = JsonDocument.Parse(JsonSerializer.Serialize(broker.Replay(0)));
        Check(replay.RootElement.GetProperty("events").GetArrayLength() == 4
            && replay.RootElement.GetProperty("currentSeq").GetInt64() == 4,
            "stale or reset inserted artificial camera pushes");
        using var metrics = JsonDocument.Parse(JsonSerializer.Serialize(broker.Diagnostics()));
        Check(metrics.RootElement.GetProperty("forwardedMotionStarts").GetInt64() == 2
            && metrics.RootElement.GetProperty("forwardedMotionClears").GetInt64() == 2,
            "stale/reset transition counters did not describe effective motion");
    }

    public static async Task NoMotionWireAndReplay()
    {
        await using var f = await Fixture.Start(motionPolicy: "none");
        var created = await f.Call("CreatePullPointSubscription");
        string path = Path(created);
        Check(Data(await f.Pull(path), "IsMotion").Single() == "false", "none policy initialized active motion");
        f.Broker.Publish(new MotionPush("MD", []));
        Check((await f.Pull(path)).Notifications.Length == 0, "none policy forwarded generic movement");
        f.Broker.Publish(new MotionPush("none", ["people", "vehicle", "dog_cat", "intrusion"]));
        var active = await f.Pull(path);
        Check(active.Notifications.Length == 3 && !Data(active, "IsMotion").Any()
            && Data(active, "ObjectType").Order().SequenceEqual(new[] { "animal", "person", "vehicle" })
            && Data(active, "State").All(value => value == "true"), "none policy suppressed or relabeled source classes");
        f.Broker.Publish(new MotionPush("MD", ["people", "vehicle", "dog_cat"]));
        Check((await f.Pull(path)).Notifications.Length == 0, "none policy duplicated active classes");
        await f.Call("SetSynchronizationPoint", path: path);
        var synchronized = await f.Pull(path);
        Check(Data(synchronized, "IsMotion").Single() == "false" && Data(synchronized, "State").All(value => value == "true"),
            "none policy synchronization lost classes or exposed motion");
        f.Broker.Publish(new MotionPush("none", []));
        var cleared = await f.Pull(path);
        Check(cleared.Notifications.Length == 3 && !Data(cleared, "IsMotion").Any()
            && Data(cleared, "State").All(value => value == "false"), "none policy lost camera class endings");
        f.Broker.Publish(new MotionPush("MD", ["people"], External: true));
        Check((await f.Pull(path)).Notifications.Length == 0, "outside control entered none-policy class events");
        using var replay = await Replay(f, "/events?after=0&limit=128");
        var rows = replay.RootElement.GetProperty("events");
        Check(rows.GetArrayLength() == 4 && rows[0].GetProperty("motion").GetBoolean()
            && rows[0].GetProperty("classes").GetArrayLength() == 0, "none policy removed or rewrote generic raw evidence");
        Check(rows[1].GetProperty("isActive").GetBoolean() && rows[1].GetProperty("classes").GetArrayLength() == 3
            && rows[1].GetProperty("rawAiTypes").GetArrayLength() == 4
            && !rows[3].GetProperty("isActive").GetBoolean(), "none policy removed class or ending replay evidence");
        using var metrics = JsonDocument.Parse(JsonSerializer.Serialize(f.Broker.Diagnostics()));
        Check(metrics.RootElement.GetProperty("motionEventPolicy").GetString() == "none"
            && metrics.RootElement.GetProperty("cameraPushes").GetInt64() == 4
            && metrics.RootElement.GetProperty("forwardedMotionStarts").GetInt64() == 0
            && metrics.RootElement.GetProperty("forwardedMotionClears").GetInt64() == 0,
            "none policy reported forwarded motion");
    }

    public static async Task NoMotionStaleResetAndReplay()
    {
        var clock = new ManualTimeProvider();
        var broker = new ProtectEventBroker(5, clock, motionEventPolicy: "none");
        var sub = broker.Subscribe(User, TimeSpan.FromMinutes(5));
        await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        broker.Publish(new MotionPush("MD", ["people"]));
        var active = await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        Check(active.Events.Count == 1 && active.Events.Single().Kind == "person" && active.Events.Single().Active,
            "none policy lost person onset or emitted motion");
        clock.Advance(TimeSpan.FromSeconds(6));
        broker.Tick();
        var stale = await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        Check(stale.Events.Count == 1 && stale.Events.Single().Kind == "person" && !stale.Events.Single().Active,
            "none policy failed to clear stale person or emitted motion");
        broker.Publish(new MotionPush("MD", ["vehicle", "dog_cat"]));
        await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        broker.ResetActive();
        var reset = await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None);
        Check(reset.Events.Count == 2 && reset.Events.All(value => value.Kind != "motion" && !value.Active),
            "none policy reset lost classes or emitted motion");
        broker.Publish(new MotionPush("MD", ["people"]));
        Check((await broker.PullAsync(sub.Id, User, TimeSpan.Zero, 10, CancellationToken.None)).Events.Single().Kind == "person",
            "none policy reconnect suppressed genuine person");
        using var replay = JsonDocument.Parse(JsonSerializer.Serialize(broker.Replay(0)));
        var rows = replay.RootElement.GetProperty("events");
        Check(rows.GetArrayLength() == 3 && rows[2].GetProperty("source").GetProperty("connectionEpoch").GetInt64() == 1,
            "none policy fabricated housekeeping pushes or lost the source reset epoch");
    }

    public static async Task JsonReplay()
    {
        await using var f = await Fixture.Start();
        using var anonymous = await f.Http.GetAsync("/events?after=0");
        Check(anonymous.StatusCode == HttpStatusCode.Unauthorized, "camera alarm replay became public");
        f.Broker.Publish(new MotionPush("none", ["people", "visitor", "intrusion"]));
        f.Broker.Publish(new MotionPush("none", []));
        // A reconnect/stale clear is bridge housekeeping and must never trigger a camera-class alarm.
        f.Broker.ResetActive();
        f.Broker.Tick();
        using var result = await Replay(f, "/events?after=0&limit=128");
        Check(result.RootElement.GetProperty("currentSeq").GetInt64() == 2
            && result.RootElement.GetProperty("oldestSeq").GetInt64() == 1
            && !result.RootElement.GetProperty("truncated").GetBoolean(), "incorrect replay cursors");
        var events = result.RootElement.GetProperty("events");
        Check(events.GetArrayLength() == 2 && events[0].GetProperty("isActive").GetBoolean()
            && events[0].GetProperty("motion").GetBoolean() && !events[1].GetProperty("isActive").GetBoolean(), "short pulse or AI-only activation lost");
        Check(events[0].GetProperty("classes")[0].GetString() == "person"
            && events[0].GetProperty("rawAiTypes").GetArrayLength() == 3
            && events[0].GetProperty("rawAiTypes")[1].GetString() == "visitor"
            && events[0].GetProperty("sourceStatus").GetString() == "none", "replay discarded raw camera evidence");
        Check(events[0].GetProperty("source").GetProperty("channel").GetInt32() == 0
            && events[0].GetProperty("zones").GetArrayLength() == 0, "source channel or honest empty zones missing");
        Check(DateTimeOffset.TryParse(events[0].GetProperty("timestampUtc").GetString(), out _), "event timestamp invalid");
        using var tail = await Replay(f, "/events?after=1&limit=1");
        Check(tail.RootElement.GetProperty("events").GetArrayLength() == 1
            && tail.RootElement.GetProperty("events")[0].GetProperty("seq").GetInt64() == 2, "replay cursor is not exclusive");
        foreach (string bad in new[] { "/events?after=-1", "/events?limit=257", "/events?after=1&after=2", "/events?other=1", "/events?after=bad" })
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, bad);
            request.Headers.TryAddWithoutValidation("Authorization", f.Basic);
            using var response = await f.Http.SendAsync(request);
            Check(response.StatusCode == HttpStatusCode.BadRequest, "malformed replay cursor accepted");
        }
        string target = "/events?after=0";
        using var challenge = await f.Http.GetAsync(target);
        string auth = Digest(challenge.Headers.GetValues("WWW-Authenticate").Single(), target);
        using var digestRequest = new HttpRequestMessage(HttpMethod.Get, target);
        digestRequest.Headers.TryAddWithoutValidation("Authorization", auth);
        using var digestResponse = await f.Http.SendAsync(digestRequest);
        Check(digestResponse.StatusCode == HttpStatusCode.OK, "Digest replay authentication failed");
        using var repeat = new HttpRequestMessage(HttpMethod.Get, target);
        repeat.Headers.TryAddWithoutValidation("Authorization", auth);
        using var refused = await f.Http.SendAsync(repeat);
        Check(refused.StatusCode == HttpStatusCode.Unauthorized, "Digest request-count replay accepted");
        for (int i = 0; i < 300; i++) f.Broker.Publish(new MotionPush("MD\n" + new string('x', 100), Enumerable.Repeat("unmapped\n" + new string('y', 100), 30).ToArray()));
        using var bounded = await Replay(f, "/events?after=0&limit=256");
        var root = bounded.RootElement;
        Check(root.GetProperty("truncated").GetBoolean() && root.GetProperty("currentSeq").GetInt64() == 302
            && root.GetProperty("oldestSeq").GetInt64() == 47 && root.GetProperty("events").GetArrayLength() == 256,
            "replay ring is unbounded or does not report its gap");
        var last = root.GetProperty("events")[255];
        Check(last.GetProperty("rawAiTypes").GetArrayLength() == 16 && last.GetProperty("rawAiTypes")[0].GetString()!.Length == 64
            && last.GetProperty("sourceStatus").GetString()!.Length == 64 && !last.GetProperty("sourceStatus").GetString()!.Contains('\n'), "raw alarm payload isn't bounded");
        await using var restarted = await Fixture.Start();
        using var cold = await Replay(restarted, "/events?after=302");
        Check(cold.RootElement.GetProperty("instanceId").GetString() != root.GetProperty("instanceId").GetString()
            && cold.RootElement.GetProperty("currentSeq").GetInt64() == 0, "restart cannot be distinguished from an empty replay tail");
    }

    private static async Task<JsonDocument> Replay(Fixture fixture, string path)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, path);
        request.Headers.TryAddWithoutValidation("Authorization", fixture.Basic);
        using var response = await fixture.Http.SendAsync(request);
        Check(response.StatusCode == HttpStatusCode.OK, "authenticated replay failed");
        string body = await response.Content.ReadAsStringAsync();
        Check(!body.Contains(User) && !body.Contains(Password), "replay includes credentials");
        return JsonDocument.Parse(body);
    }
    private static string Digest(string challenge, string target)
    {
        string Value(string key) => Regex.Match(challenge, key + "=\"([^\"]+)\"").Groups[1].Value;
        string nonce = Value("nonce"), realm = Value("realm");
        string Md5(string input) => Convert.ToHexString(MD5.HashData(Encoding.UTF8.GetBytes(input))).ToLowerInvariant();
        string hash = Md5(Md5(User + ":" + realm + ":" + Password) + ":" + nonce + ":00000001:test-client:auth:" + Md5("GET:" + target));
        return $"Digest username=\"{User}\", realm=\"{realm}\", nonce=\"{nonce}\", uri=\"{target}\", response=\"{hash}\", qop=auth, nc=00000001, cnonce=\"test-client\"";
    }

    private static IEnumerable<string> Data(Reply reply, string name) => reply.Xml.Descendants(XName.Get("SimpleItem", ProtectOnvifServer.NsSchema))
        .Where(e => e.Attribute("Name")?.Value == name).Select(e => e.Attribute("Value")!.Value);
    private sealed record Reply(int Status, string Body, XElement Xml)
    {
        public XElement[] Notifications => Xml.Descendants(XName.Get("NotificationMessage", ProtectOnvifServer.NsWsnt)).ToArray();
    }
    private sealed class Fixture : IAsyncDisposable
    {
        public required ProtectOnvifServer Server;
        public ProtectEventBroker Broker => Server.Events!;
        public required HttpClient Http;
        public required CancellationTokenSource Stop;
        public required Task Serving;
        public string Basic => "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(User + ":" + Password));
        public static async Task<Fixture> Start(bool enabled = true, string motionPolicy = "all")
        {
            var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port; listener.Stop();
            var config = new BridgeOnvifConfig
            {
                Port = port, Bind = "127.0.0.1", AdvertisedHost = "127.0.0.1", Mac = "02:54:45:53:55:01",
                Name = "Event test", Model = "Synthetic", Uuid = Guid.NewGuid().ToString("D"), Discovery = false, Events = enabled,
                MotionEventPolicy = motionPolicy,
                Profiles = [new BridgeOnvifProfile { Width = 1920, Height = 1080, Codec = "H264", Fps = 20 }]
            };
            var server = new ProtectOnvifServer(config, new Dictionary<string, string> { [User] = Password, ["events-other"] = Password },
                [new ProtectOnvifStream("main", "/test/mainStream", new TestHub("events-test"))], port == 18554 ? 18555 : 18554);
            var stop = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var fixture = new Fixture { Server = server, Stop = stop, Serving = server.RunAsync(stop.Token),
                Http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}"), Timeout = TimeSpan.FromSeconds(5) } };
            for (int n = 0; n < 30; n++)
            {
                if (fixture.Serving.IsFaulted) await fixture.Serving;
                try { using var ready = await fixture.Http.GetAsync("/health"); return fixture; }
                catch (HttpRequestException) { await Task.Delay(10); }
            }
            throw new Exception("Event listener did not start");
        }
        public Task<Reply> Pull(string path) => Call("PullMessages", "<e:Timeout>PT0S</e:Timeout><e:MessageLimit>64</e:MessageLimit>", path: path);
        public async Task<Reply> Call(string action, string content = "", string? ns = null, string path = EventPath,
            bool authenticate = true, string user = User, string? security = null, string? authorization = null)
        {
            ns ??= ProtectOnvifServer.NsEvents;
            string id = "urn:uuid:" + Guid.NewGuid().ToString("D");
            string soap = $"<s:Envelope xmlns:s=\"{ProtectOnvifServer.NsSoap}\" xmlns:wsa=\"{ProtectOnvifServer.NsWsa}\"><s:Header>" +
                $"<wsa:MessageID>{id}</wsa:MessageID>" + (authorization != null ? "" : security ?? (authenticate ? Token(user) : "")) +
                $"</s:Header><s:Body><e:{action} xmlns:e=\"{ns}\">{content}</e:{action}></s:Body></s:Envelope>";
            using var request = new HttpRequestMessage(HttpMethod.Post, path) { Content = new StringContent(soap, Encoding.UTF8, "application/soap+xml") };
            if (authorization != null) request.Headers.TryAddWithoutValidation("Authorization", authorization);
            using var response = await Http.SendAsync(request);
            string body = await response.Content.ReadAsStringAsync();
            var xml = XElement.Parse(body);
            if (response.IsSuccessStatusCode && ns is ProtectOnvifServer.NsEvents or ProtectOnvifServer.NsWsnt)
                Check(Node(xml, "RelatesTo").Value == id, "event response lost WS-Addressing correlation");
            return new((int)response.StatusCode, body, xml);
        }
        public static string Token(string user)
        {
            const string wsse = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd";
            const string wsu = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd";
            byte[] nonce = RandomNumberGenerator.GetBytes(24);
            string created = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
            string digest = OnvifClient.PasswordDigest(nonce, created, Password);
            return $"<wsse:Security xmlns:wsse=\"{wsse}\" xmlns:wsu=\"{wsu}\"><wsse:UsernameToken><wsse:Username>{Esc(user)}</wsse:Username>" +
                $"<wsse:Password Type=\"{wsse}#PasswordDigest\">{digest}</wsse:Password><wsse:Nonce>{Convert.ToBase64String(nonce)}</wsse:Nonce>" +
                $"<wsu:Created>{created}</wsu:Created></wsse:UsernameToken></wsse:Security>";
        }
        public async ValueTask DisposeAsync()
        {
            Stop.Cancel();
            try { await Serving.WaitAsync(TimeSpan.FromSeconds(2)); } catch (OperationCanceledException) { }
            Http.Dispose(); Stop.Dispose();
        }
    }
}
