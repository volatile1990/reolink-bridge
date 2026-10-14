using System.Reflection;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Xml.Linq;
using Neolink.Bc;
using Neolink.Bc.Xml;
using Neolink.Protocol;
using Neolink.Streaming;

internal static class ProtectZoneDiscoveryTests
{
    public static async Task RunAsync()
    {
        var control = DispatchProxy.Create<ICameraControl, ZoneSource>(); var source = (ZoneSource)(object)control;
        var panel = new ProtectCameraControls("synthetic-zone-source", control);
        var first = JsonSerializer.SerializeToElement(await panel.ReadAsync(null, null, CancellationToken.None), ProtectCameraControls.JsonOptions);
        var zone = first.GetProperty("resources").EnumerateArray().Single(r => r.GetProperty("id").GetString() == "zone");
        string?[] offered = zone.GetProperty("fields").EnumerateArray().Single(f => f.GetProperty("id").GetString() == "type")
            .GetProperty("options").EnumerateArray().Select(o => o.GetProperty("value").GetString()).ToArray();
        Check(offered.SequenceEqual(new[] { "md", "people", "dog_cat" }), "Initial selector missed real AI grids or invented an unsupported vehicle grid");
        Check(source.Queries.Take(3).SequenceEqual(new[] { "people", "vehicle", "dog_cat" }), "Discovery probed arbitrary types or used unsupported native settings");
        int aiQueries = source.Queries.Count(t => t != "md");
        await panel.ReadAsync(null, null, CancellationToken.None);
        Check(source.Queries.Count(t => t != "md") == aiQueries, "Repeated state reads reprobed settled AI grid types");
        var absent = JsonSerializer.SerializeToElement(await panel.ReadAsync("zone", "vehicle", CancellationToken.None), ProtectCameraControls.JsonOptions);
        Check(!absent.GetProperty("descriptor").GetProperty("supported").GetBoolean(), "Explicit unsupported grid was fabricated");

        var cancelledControl = DispatchProxy.Create<ICameraControl, ZoneSource>(); var cancelledSource = (ZoneSource)(object)cancelledControl;
        cancelledSource.DelayProbe = true;
        var cancelledPanel = new ProtectCameraControls("synthetic-cancel-zone", cancelledControl);
        using var stop = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        bool cancelled = false;
        try { await cancelledPanel.ReadAsync(null, null, stop.Token); } catch (OperationCanceledException) { cancelled = true; }
        Check(cancelled, "Cancellation of native zone discovery was ignored");
        cancelledSource.DelayProbe = false;
        var retry = JsonSerializer.SerializeToElement(await cancelledPanel.ReadAsync(null, null, CancellationToken.None), ProtectCameraControls.JsonOptions);
        Check(retry.GetProperty("resources").EnumerateArray().Single(r => r.GetProperty("id").GetString() == "zone")
            .GetProperty("fields").EnumerateArray().Single(f => f.GetProperty("id").GetString() == "type")
            .GetProperty("options").EnumerateArray().Any(o => o.GetProperty("value").GetString() == "people"),
            "Cancelled read stranded request gate or suppressed the next discovery");
        await DualTransportFlow(["people", "vehicle", "dog_cat"]);
        await DualTransportFlow(["people", "dog_cat"]);
        Console.WriteLine("Protect initial zone discovery exposes only confirmed native grids, throttles reads and releases cancellation.");
    }

    private static string?[] Offered(JsonElement response) => response.GetProperty("resources").EnumerateArray()
        .Single(r => r.GetProperty("id").GetString() == "zone").GetProperty("fields").EnumerateArray()
        .Single(f => f.GetProperty("id").GetString() == "type").GetProperty("options").EnumerateArray()
        .Select(o => o.GetProperty("value").GetString()).ToArray();

    private static async Task DualTransportFlow(string[] types)
    {
        // Exercise the real CameraControl, not an ICameraControl fake: CGI owns
        // sensitivity/target size but omits scope; cmd342 alone owns the AI grid.
        var bc = DispatchProxy.Create<IBcCamera, DualCamera>(); var native = (DualCamera)(object)bc;
        foreach (string type in types)
            native.Configs[type] = XElement.Parse($"<AiDetectCfg version='1.1'><chn>0</chn><type>{type}</type><sensitivity>45</sensitivity><stayTime>0</stayTime><width>2</width><height>2</height><area>oA==</area><vendor>keep</vendor></AiDetectCfg>");
        var live = DispatchProxy.Create<ILiveCameraSource, ProtectCameraControlTests.LiveReadProxy>();
        ((ProtectCameraControlTests.LiveReadProxy)(object)live).Camera = bc;
        using var api = new ReolinkHttpApi("http://127.0.0.1", "synthetic", "synthetic", 0);
        using var handler = new DualHttp(types); using var client = new HttpClient(handler);
        typeof(ReolinkHttpApi).GetField("_http", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(api, client);
        var control = new CameraControl(live, httpApi: api);
        typeof(CameraControl).GetField("_caps", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(control,
            new CameraCapabilities(null, null, new CameraFeatures(false, false, false, false, false)));
        typeof(CameraControl).GetField("_capsSession", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(control, bc);
        var panel = new ProtectCameraControls("synthetic-dual-zone", control);
        string[] expected = ["md", .. types];
        JsonElement Serialize(object state) => JsonSerializer.SerializeToElement(state, ProtectCameraControls.JsonOptions);
        Check(Offered(Serialize(await panel.ReadAsync(null, null, CancellationToken.None))).SequenceEqual(expected),
            "First real full state lost BC grids to later CGI parameter reads");
        Check(native.Writes == 0 && handler.Writes == 0, "Discovery changed the camera");
        var sensitivities = await control.GetAiSensitivitiesAsync(CancellationToken.None);
        var parameters = await control.GetAiAlarmParametersAsync("people", CancellationToken.None);
        Check(sensitivities?.Select(s => s.Type).SequenceEqual(types) == true && parameters?.Fields.Single(f => f.Id == "sensitivity").Value == 45,
            "CGI parameter source was bypassed or invented an unsupported class");
        Check(control.ZoneTypes().SequenceEqual(expected), "CGI without scope erased a genuine BC grid");
        int reads = native.AiReads;
        for (int i = 0; i < 3; i++)
            Check(Offered(Serialize(await panel.ReadAsync(null, null, CancellationToken.None))).SequenceEqual(expected),
                "Repeated full state within discovery throttle lost confirmed AI grids");
        // Full state includes one explicit read-only AI diagnostic per refresh.
        Check(native.AiReads == reads + 3, "Settled grids caused repeated cmd342 discovery inside the 30s throttle");
        Check(native.Writes == 0 && handler.Writes == 0, "A status refresh performed a camera write");
        if (!types.Contains("vehicle"))
        {
            int before = native.AiReads;
            var missing = Serialize(await panel.ReadAsync("zone", "vehicle", CancellationToken.None));
            Check(!missing.GetProperty("descriptor").GetProperty("supported").GetBoolean() && native.AiReads == before,
                "E1-like camera fabricated or probed an unsupported vehicle grid");
        }
        // The last parameter read chose CGI. A zone write must still use BC342/343
        // and preserve the original settings rather than send a nonexistent CGI scope.
        await control.GetAiAlarmParametersAsync("people", CancellationToken.None);
        await control.GetDetectionZoneAsync("people", CancellationToken.None);
        handler.AllowParameterWrite = true;
        await control.SetAiSensitivityAsync("people", 45, CancellationToken.None);
        handler.AllowParameterWrite = false;
        Check(handler.Writes == 1 && native.Writes == 0, "A pure BC zone read changed the separately proven CGI parameter writer");
        await panel.WriteAsync("zone", "{\"values\":{\"type\":\"people\",\"table\":\"1010\"}}", CancellationToken.None);
        Check(native.Writes == 1 && handler.Writes == 1 && native.Configs["people"].Element("vendor")?.Value == "keep",
            "CGI sensitivity read changed the independent BC zone write transport or its RMW fields");
        native.Configs["people"].Element("area")!.Remove();
        Check(await control.GetDetectionZoneAsync("people", CancellationToken.None) == null
            && !control.ZoneTypes().Contains("people") && control.ZoneTypes().Contains("dog_cat"),
            "A genuine later BC no-grid answer failed to clear only its own evidence");
        handler.PeopleGrid = true;
        Check(await control.GetDetectionZoneAsync("people", CancellationToken.None) is { Table: "1010" }, "Real CGI grid omitted");
        handler.RejectPeople = true;
        await control.GetAiAlarmParametersAsync("people", CancellationToken.None);
        Check(control.ZoneTypes().Contains("people"), "BC parameters without area erased a genuine CGI grid");
    }

    private sealed class DualHttp(string[] types) : HttpMessageHandler
    {
        public int Writes;
        public bool PeopleGrid;
        public bool RejectPeople;
        public bool AllowParameterWrite;
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var input = JsonNode.Parse(await request.Content!.ReadAsStringAsync(ct))![0]!;
            string command = input["cmd"]!.GetValue<string>();
            if (command.StartsWith("Set", StringComparison.Ordinal))
            {
                Check(AllowParameterWrite && command == "SetAiAlarm" && input["param"]!["AiAlarm"]!["scope"] == null,
                    "BC-only grid incorrectly sent a CGI write");
                Writes++;
            }
            JsonObject? value = null, range = new();
            switch (command)
            {
                case "SetAiAlarm": value = new(); break;
                case "Login": value = new() { ["Token"] = new JsonObject { ["name"] = "synthetic-token", ["leaseTime"] = 3600 } }; break;
                case "GetAbility": value = new() { ["Ability"] = new JsonObject() }; break;
                case "GetAiState":
                    var state = new JsonObject { ["channel"] = 0 };
                    foreach (string type in CameraControl.AiAlarmTypes) state[type] = new JsonObject { ["support"] = types.Contains(type) ? 1 : 0, ["alarm_state"] = 0 };
                    value = state; break;
                case "GetAiAlarm":
                    string kind = input["param"]!["ai_type"]!.GetValue<string>();
                    if (!types.Contains(kind) || kind == "people" && RejectPeople) break;
                    var alarm = new JsonObject { ["channel"] = 0, ["ai_type"] = kind, ["sensitivity"] = 45, ["stay_time"] = 0 };
                    if (kind == "people" && PeopleGrid) alarm["scope"] = Grid();
                    value = new() { ["AiAlarm"] = alarm };
                    range["AiAlarm"] = new JsonObject { ["sensitivity"] = new JsonObject { ["min"] = 0, ["max"] = 100 } }; break;
                case "GetMdAlarm": value = new() { ["MdAlarm"] = new JsonObject { ["channel"] = 0, ["scope"] = Grid() } }; break;
            }
            var reply = new JsonObject { ["cmd"] = command, ["code"] = value == null ? 1 : 0, ["value"] = value, ["range"] = range };
            if (value == null) reply["error"] = new JsonObject { ["rspCode"] = -1 };
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(new JsonArray(reply).ToJsonString(), Encoding.UTF8, "application/json") };
        }
        private static JsonObject Grid() => new() { ["cols"] = 2, ["rows"] = 2, ["table"] = "1010" };
    }

    public class DualCamera : DispatchProxy
    {
        public Dictionary<string, XElement> Configs { get; } = [];
        public int AiReads;
        public int Writes;
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == "get_ChannelId") return (byte)0;
            Check(method.Name == "SendCommandAsync", "Full state opened another native session/login or actuator");
            uint id = (uint)args![0]!;
            if (id is not (342 or 343))
            {
                Check(id is 26 or 264 or 56 or 46 or 44 or 36, "Full state requested an unexpected native actuator/login");
                return Task.FromException<BcMessage?>(new CameraCommandException(id, 400));
            }
            var request = (BcXmlBody)args[1]!; string type = request.Raw.Single().Element("type")!.Value;
            Check(((ExtensionXml)args[2]!).ChannelId == 0, "AI discovery used another camera channel");
            if (!Configs.TryGetValue(type, out var cfg)) return Task.FromException<BcMessage?>(new CameraCommandException(id, 400));
            if (id == 343) { Writes++; Configs[type] = new XElement(request.Raw.Single()); }
            else AiReads++;
            var body = new BcXmlBody(); if (id == 342) body.Raw.Add(new XElement(Configs[type]));
            return Task.FromResult<BcMessage?>(new BcMessage { Meta = new BcMeta { MsgId = id, ResponseCode = 200 }, Xml = body });
        }
    }
    private static void Check(bool value, string message) { if (!value) throw new Exception(message); }

    public class ZoneSource : DispatchProxy
    {
        public List<string> Queries { get; } = [];
        private readonly HashSet<string> _confirmed = [];
        public bool DelayProbe;
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            string name = method!.Name;
            if (name.StartsWith("Set", StringComparison.Ordinal) || name is "PtzAsync" or "SirenAsync" or "TalkAsync")
                throw new Exception("Read-only zone discovery attempted an actuator");
            if (name == "ZoneTypes") return new[] { "md" }.Concat(new[] { "people", "vehicle", "dog_cat" }.Where(_confirmed.Contains)).ToArray();
            if (name == "GetDetectionZoneAsync")
            {
                string type = (string)args![0]!; Queries.Add(type);
                if (DelayProbe && type != "md") return Delayed((CancellationToken)args[1]!);
                if (type == "vehicle") return Task.FromResult<DetectionZone?>(null);
                if (type != "md") _confirmed.Add(type);
                return Task.FromResult<DetectionZone?>(new(type, 2, 2, "1010"));
            }
            if (method.ReturnType.IsGenericType && method.ReturnType.GetGenericTypeDefinition() == typeof(Task<>))
            {
                object? value = name switch
                {
                    "GetCapabilitiesAsync" => new CameraCapabilities(null, null, new CameraFeatures(false, false, false, false, false)),
                    "GetHttpFeaturesAsync" => new HttpFeatures(null, null, null, null, null, null, null, AiSensitivities: [new("people", 30, null), new("vehicle", 30, null), new("dog_cat", 30, null)]),
                    "GetAiSensitivitiesAsync" => (IReadOnlyList<AiSensitivity>)new AiSensitivity[] { new("people", 30, null), new("vehicle", 30, null), new("dog_cat", 30, null) },
                    _ => null
                };
                Type result = method.ReturnType.GenericTypeArguments[0];
                return typeof(Task).GetMethod(nameof(Task.FromResult))!.MakeGenericMethod(result).Invoke(null, [value]);
            }
            return method.ReturnType == typeof(bool) ? false : null;
        }
        private static async Task<DetectionZone?> Delayed(CancellationToken ct) { await Task.Delay(Timeout.Infinite, ct); return null; }
    }
}
