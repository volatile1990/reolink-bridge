using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Xml.Linq;
using Neolink.Config;
using Neolink.Onvif;
using Neolink.Streaming;
using Neolink.Protocol;
using Neolink.Bc;
using Neolink.Bc.Xml;

internal static class ProtectCameraControlTests
{
    public static async Task RunAsync()
    {
        var aiSource = JsonNode.Parse("{\"channel\":0,\"ai_type\":\"people\",\"sensitivity\":50,\"stay_time\":0,\"widthMin\":10,\"widthMax\":80,\"enable\":1,\"unknown\":{\"keep\":true}}")!.AsObject();
        var aiRange = JsonNode.Parse("{\"stay_time\":{\"min\":0,\"max\":8},\"widthMin\":{\"min\":0,\"max\":100},\"widthMax\":{\"min\":0,\"max\":100}}")!.AsObject();
        CameraControl.ApplyAiParameters("people", aiSource, aiRange, new Dictionary<string, double> { ["stayTime"] = 3 }, false);
        if (aiSource["stay_time"]!.GetValue<int>() != 3 || aiSource["enable"]!.GetValue<int>() != 0 || aiSource["unknown"]?["keep"]?.GetValue<bool>() != true)
            throw new Exception("AI read-modify-write failed to preserve unrelated source config");
        string prior = aiSource.ToJsonString();
        bool crossed = false;
        try { CameraControl.ApplyAiParameters("people", aiSource, aiRange, new Dictionary<string, double> { ["widthMin"] = 90 }, null); }
        catch (ArgumentException) { crossed = true; }
        if (!crossed || prior != aiSource.ToJsonString()) throw new Exception("Invalid AI sizes partially changed config");
        if (CameraControl.ParseAiParameters("people", aiSource, null).Fields.Single(f => f.Id == "stayTime").Writable)
            throw new Exception("Unreported AI bounds were invented");
        var modern = JsonNode.Parse("{\"channel\":0,\"ai_type\":\"dog_cat\",\"sensitivity\":60,\"stay_time\":0,\"min_target_width\":0.0775189995765686,\"min_target_height\":0.04797599837183952,\"max_target_width\":0.3248060047626495,\"max_target_height\":0.4674420058727264,\"scope\":{\"cols\":1,\"rows\":1,\"table\":\"1\"},\"unknown\":true}")!.AsObject();
        var modernRange = JsonNode.Parse("{\"min_target_width\":0.0,\"min_target_height\":0.0,\"max_target_width\":1.0,\"max_target_height\":1.0,\"sensitivity\":{\"min\":0,\"max\":100},\"stay_time\":{\"min\":0,\"max\":8}}")!.AsObject();
        var parsedModern = CameraControl.ParseAiParameters("dog_cat", modern, modernRange);
        if (parsedModern.Fields.Count != 6 || parsedModern.Fields.Single(f => f.Id == "widthMin") is not { Writable: true, Step: 0.000001, Min: 0, Max: 1 })
            throw new Exception("Actual normalized firmware bounds were not recognized");
        string originalHeight = modern["min_target_height"]!.ToJsonString(), originalScope = modern["scope"]!.ToJsonString();
        CameraControl.ApplyAiParameters("dog_cat", modern, modernRange, new Dictionary<string, double> { ["widthMin"] = 0.123456, ["sensitivity"] = 65 }, null);
        if (modern["min_target_width"]!.GetValue<double>() != 0.123456 || modern.ContainsKey("widthMin") || modern["sensitivity"]!.GetValue<int>() != 65
            || modern["min_target_height"]!.ToJsonString() != originalHeight || modern["scope"]!.ToJsonString() != originalScope || modern["unknown"]?.GetValue<bool>() != true)
            throw new Exception("Normalized AI writes altered the source dialect or untouched settings");
        string modernBefore = modern.ToJsonString();
        foreach (var invalid in new Dictionary<string, double>[] { new() { ["widthMin"] = 0.9 }, new() { ["widthMax"] = 1.1 }, new() { ["stayTime"] = 1.5 }, new() { ["widthMin"] = double.NaN } })
        {
            bool failed = false;
            try { CameraControl.ApplyAiParameters("dog_cat", modern, modernRange, invalid, null); } catch (ArgumentException) { failed = true; }
            if (!failed || modern.ToJsonString() != modernBefore) throw new Exception("Invalid AI range partially modified source config");
        }
        var fractionalSchema = new ProtectControlResource("ai", "AI", "AI", true, true, false,
            [new("type", "Type", "select", Options: [new("dog_cat", "Animal")]), new("widthMin", "Width", "number", 0, 1, 0.000001)]);
        ProtectCameraControls.ValidateWrite(fractionalSchema, JsonNode.Parse("{\"type\":\"dog_cat\",\"widthMin\":0.123456}")!.AsObject());
        var bc = DispatchProxy.Create<IBcCamera, BcReadProxy>(); var bcProbe = (BcReadProxy)(object)bc;
        var live = DispatchProxy.Create<ILiveCameraSource, LiveReadProxy>(); ((LiveReadProxy)(object)live).Camera = bc;
        var sourceControl = new CameraControl(live);
        var aiDiagnostic = await sourceControl.GetBaichuanAiDiagnosticAsync("people", CancellationToken.None);
        if (bcProbe.Commands.Count != 1 || bcProbe.Commands[0] != 342 || aiDiagnostic?.NumericFields["sensitivity"] != "75"
            || JsonSerializer.Serialize(aiDiagnostic).Contains("PRIVATE-SENTINEL"))
            throw new Exception("AI diagnostics did not reuse exactly the live read-only command or leaked raw XML");
        var control = DispatchProxy.Create<ICameraControl, ControlProxy>();
        var probe = (ControlProxy)(object)control;
        var config = Config();
        var service = new ProtectCameraControls("Synthetic camera", control);
        var server = new ProtectOnvifServer(config, new Dictionary<string, string> { ["user"] = "synthetic" },
            [new("main", "/synthetic/mainStream", new StreamHub("test"))], 18554, controls: service);
        probe.Calls.Clear(); // Event subscription during construction performs no camera I/O.
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        { ["Authorization"] = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("user:synthetic")),
            ["Content-Type"] = "application/json", ["X-Reolink-Control-Intent"] = "user" };
        void Check(bool yes, string what) { if (!yes) throw new Exception(what); }
        async Task<(int Status, string Body)> Request(string method, string path, string body,
            IReadOnlyDictionary<string, string>? h = null) => await server.HandleControlAsync(method, path, body, h ?? headers, CancellationToken.None);

        var denied = await Request("POST", "/control/siren", "{\"values\":{\"on\":true}}", new Dictionary<string, string>());
        Check(denied.Status == 401 && probe.Calls.Count == 0, "Unauthorized requests touched the camera");
        var noIntent = new Dictionary<string, string>(headers); noIntent.Remove("X-Reolink-Control-Intent");
        denied = await Request("POST", "/control/siren", "{\"values\":{\"on\":true}}", noIntent);
        Check(denied.Status == 403 && probe.Calls.Count == 0, "Missing intent touched the camera");
        var origin = new Dictionary<string, string>(headers) { ["Origin"] = "https://example.invalid" };
        denied = await Request("POST", "/control/siren", "{\"values\":{\"on\":true}}", origin);
        Check(denied.Status == 403 && probe.Calls.Count == 0, "Direct browser writes were accepted");

        var read = await Request("GET", "/control/state", "");
        Check(read.Status == 200 && !read.Body.Contains("PRIVATE-SENTINEL"), "Raw support XML leaked");
        Check(probe.Writes == 0 && !probe.Calls.Contains("SirenAsync"), "Reading capabilities triggered an actuator");
        using var doc = JsonDocument.Parse(read.Body);
        var descriptors = doc.RootElement.GetProperty("resources").EnumerateArray().ToArray();
        Check(descriptors.First(r => r.GetProperty("id").GetString() == "siren").GetProperty("isActuator").GetBoolean(), "Siren lost actuator marker");
        Check(!descriptors.First(r => r.GetProperty("id").GetString() == "privacy").GetProperty("supported").GetBoolean(), "Unsupported privacy was advertised");
        Check(doc.RootElement.GetProperty("values").GetProperty("siren").GetProperty("on").ValueKind == JsonValueKind.Null,
            "Unknown siren state was invented");

        foreach (string body in new[] { "{\"values\":{\"volume\":101}}", "{\"values\":{\"volume\":-1}}",
            "{\"values\":{\"volume\":1.5}}", "{\"values\":{\"volume\":\"10\"}}",
            "{\"values\":{\"volume\":10,\"password\":\"secret\"}}", "{\"values\":{\"volume\":1,\"volume\":2}}",
            "{\"values\":{\"volume\":1},\"other\":true}" })
        {
            var bad = await Request("POST", "/control/volume", body);
            Check(bad.Status == 400 && probe.Writes == 0, "Invalid schema reached a write");
        }
        var wrongZone = await Request("POST", "/control/zone", "{\"values\":{\"type\":\"md\",\"table\":\"01\"}}");
        Check(wrongZone.Status == 400 && probe.Writes == 0, "Mismatched zone dimensions were written");
        var wrongLed = await Request("POST", "/control/led", "{\"values\":{\"state\":\"on\"}}");
        Check(wrongLed.Status == 400 && probe.Writes == 0, "Incorrect LED protocol dialect was accepted");
        var wrongType = await Request("POST", "/control/ai", "{\"values\":{\"type\":\"face\",\"sensitivity\":20}}");
        Check(wrongType.Status == 422 && probe.Writes == 0, "Unreported AI type was written");
        var readonlyField = await Request("POST", "/control/ai", "{\"values\":{\"type\":\"people\",\"sensitivity\":20,\"stayTime\":2}}");
        Check(readonlyField.Status == 400 && probe.Writes == 0, "Read-only AI delay was written");
        var unknown = await Request("POST", "/control/network", "{\"values\":{\"enable\":false}}");
        Check(unknown.Status == 400 && probe.Writes == 0, "Unknown network resource reached a write");
        var serviceWrite = await Request("POST", "/control/services", "{\"values\":{\"httpsEnabled\":true}}");
        Check(serviceWrite.Status == 422 && probe.Writes == 0, "Read-only service diagnostic allowed a network mutation");
        var aiRead = await Request("GET", "/control/ai?type=people", "");
        Check(aiRead.Status == 200 && aiRead.Body.Contains("\"type\":\"people\""), "Selected AI type was not read");
        var aiWrite = await Request("POST", "/control/ai", "{\"values\":{\"type\":\"people\",\"sensitivity\":65}}");
        Check(aiWrite.Status == 200 && probe.Writes == 1 && probe.LastAiSensitivity == 65, "Valid AI write was not dispatched");
        var volume = await Request("POST", "/control/volume", "{\"values\":{\"volume\":45}}");
        Check(volume.Status == 200 && probe.Writes == 2 && volume.Body.Contains("\"volume\":45"), "Write did not return read-back state");
        var failure = await Request("POST", "/control/siren", "{\"values\":{\"on\":false}}");
        Check(failure.Status == 502 && !failure.Body.Contains("PRIVATE-SENTINEL"), "Camera exception leaked secret content");
        string Soap(string action, string inner = "") => $"<s:Envelope xmlns:s=\"{ProtectOnvifServer.NsSoap}\"><s:Body><p:{action} xmlns:p=\"{ProtectOnvifServer.NsPtz}\">{inner}</p:{action}></s:Body></s:Envelope>";
        int priorWrites = probe.Writes, priorCalls = probe.Calls.Count;
        var unauthPtz = await server.HandleAsync(Soap("ContinuousMove", "<ProfileToken>main</ProfileToken>"), null, CancellationToken.None);
        Check(unauthPtz.Status == 401 && probe.Calls.Count == priorCalls, "Unauthenticated native PTZ touched camera");
        var status = await server.HandleAsync(Soap("GetStatus", "<ProfileToken>main</ProfileToken>"), headers["Authorization"], CancellationToken.None);
        Check(status.Status == 200 && !status.Body.Contains("<tt:Position>") && probe.Writes == priorWrites, "Native PTZ status fabricated a position or moved camera");
        var options = await server.HandleAsync(Soap("GetConfigurationOptions", "<ConfigurationToken>ptz_main</ConfigurationToken>"), headers["Authorization"], CancellationToken.None);
        Check(options.Status == 200 && options.Body.Contains("RelativePanTiltTranslationSpace") && !options.Body.Contains("AbsolutePanTiltPositionSpace"), "Native PTZ advertised unavailable absolute pan/tilt");
        var profiles = await server.HandleAsync($"<s:Envelope xmlns:s=\"{ProtectOnvifServer.NsSoap}\"><s:Body><m:GetProfiles xmlns:m=\"{ProtectOnvifServer.NsMedia}\"/></s:Body></s:Envelope>", headers["Authorization"], CancellationToken.None);
        Check(profiles.Status == 200 && XDocument.Parse(profiles.Body).Descendants().Any(e => e.Name.LocalName == "PTZConfiguration"), "Actual PTZ capability missing from existing profile");
        foreach (string invalidMove in new[] { "<Translation><PanTilt x=\"NaN\" y=\"0\"/></Translation>",
            "<Translation><PanTilt x=\"2\" y=\"0\"/></Translation>", "<Translation><PanTilt x=\"1\"/></Translation>" })
        {
            var rejectedMove = await server.HandleAsync(Soap("RelativeMove", "<ProfileToken>main</ProfileToken>" + invalidMove), headers["Authorization"], CancellationToken.None);
            Check(rejectedMove.Status == 400 && probe.Writes == priorWrites, "Invalid native movement reached a write");
        }
        var absoluteHead = await server.HandleAsync(Soap("AbsoluteMove", "<ProfileToken>main</ProfileToken><Position><PanTilt x=\"0\" y=\"0\"/></Position>"), headers["Authorization"], CancellationToken.None);
        Check(absoluteHead.Status == 400 && probe.Writes == priorWrites, "Unsupported absolute pan/tilt was translated");
        var relative = await server.HandleAsync(Soap("RelativeMove", "<ProfileToken>main</ProfileToken><Translation><PanTilt x=\"0.05\" y=\"0\"/></Translation>"), headers["Authorization"], CancellationToken.None);
        Check(relative.Status == 200 && probe.Writes == priorWrites + 2 && probe.LastPtz == "stop", "Relative move did not send bounded motion followed by stop");
        int boundedWrites = probe.Writes;
        var badDuration = await Request("POST", "/control/ptz", "{\"values\":{\"command\":\"up\",\"durationMs\":1001}}");
        Check(badDuration.Status == 400 && probe.Writes == boundedWrites, "Excessive REST PTZ duration was accepted");
        var restPtz = await Request("POST", "/control/ptz", "{\"values\":{\"command\":\"up\",\"durationMs\":50}}");
        Check(restPtz.Status == 200 && probe.Writes == boundedWrites + 2 && probe.LastPtz == "stop", "REST PTZ pulse did not stop");
        probe.FailNextPtzDirection = true;
        int beforeFailure = probe.Writes;
        var failedPulse = await Request("POST", "/control/ptz", "{\"values\":{\"command\":\"right\",\"durationMs\":50}}");
        Check(failedPulse.Status == 502 && probe.Writes == beforeFailure + 2 && probe.LastPtz == "stop", "Failed direction did not attempt an independent stop");
        using (var pulseCancel = new CancellationTokenSource())
        {
            int beforeCancel = probe.Writes;
            var pulse = service.WriteAsync("ptz", "{\"values\":{\"command\":\"down\",\"durationMs\":1000}}", pulseCancel.Token);
            for (int tries = 0; tries < 100 && probe.LastPtz != "down"; tries++) await Task.Delay(5);
            Check(probe.LastPtz == "down", "Cancelled pulse did not begin");
            pulseCancel.Cancel();
            bool cancelled = false; try { await pulse; } catch (OperationCanceledException) { cancelled = true; }
            Check(cancelled && probe.Writes == beforeCancel + 2 && probe.LastPtz == "stop", "Cancellation abandoned a moving PTZ head");
        }
        int beforeStop = probe.Writes;
        var longPulse = service.WriteAsync("ptz", "{\"values\":{\"command\":\"left\",\"durationMs\":1000}}", CancellationToken.None);
        for (int tries = 0; tries < 100 && probe.LastPtz != "left"; tries++) await Task.Delay(5);
        Check(probe.LastPtz == "left", "Concurrent stop test pulse did not begin");
        await service.WriteAsync("ptz", "{\"values\":{\"command\":\"stop\"}}", CancellationToken.None);
        Check(!longPulse.IsCompleted && probe.LastPtz == "stop", "Stop waited behind pulse duration");
        await longPulse;
        Check(probe.Writes == beforeStop + 2, "Finished older pulse stopped a newer owner again");
        boundedWrites = probe.Writes;
        probe.SupportsPrivacy = true;
        var privacyRead = await Request("GET", "/control/privacy", "");
        Check(privacyRead.Status == 200 && JsonDocument.Parse(privacyRead.Body).RootElement.GetProperty("descriptor").GetProperty("reason").GetString() == "Der Privatsphärenmodus kann hier nur ausgelesen werden."
            && privacyRead.Body.Contains("\"writable\":false"), "Privacy policy guard missing");
        var privacyWrite = await Request("POST", "/control/privacy", "{\"values\":{\"on\":true}}");
        Check(privacyWrite.Status == 422 && probe.Writes == boundedWrites, "Privacy was enabled without a watchdog policy");
        probe.SupportsPrivacy = false;
        probe.SupportsPtz = false;
        var noPtz = await server.HandleAsync(Soap("GetConfigurations"), headers["Authorization"], CancellationToken.None);
        Check(noPtz.Status == 400, "Native PTZ did not honor actual capability change");
        config.Controls = false;
        var disabled = new ProtectOnvifServer(config, new Dictionary<string, string> { ["user"] = "synthetic" },
            [new("main", "/synthetic/mainStream", new StreamHub("test"))], 18554);
        Check((await disabled.HandleControlAsync("GET", "/control/state", "", headers, CancellationToken.None)).Status == 404,
            "Controls were not opt-in");
        bool rejected = false;
        config.Controls = true;
        try { _ = new ProtectOnvifServer(config, new Dictionary<string, string>(),
            [new("main", "/synthetic/mainStream", new StreamHub("test"))], 18554); }
        catch (ArgumentException) { rejected = true; }
        Check(rejected, "Controls accepted unauthenticated bridge configuration");
        Console.WriteLine("Protect control schema/authentication/capability/write-readback tests passed; synthetic control only.");
    }

    private static BridgeOnvifConfig Config() => new()
    {
        Bind = "127.0.0.1", AdvertisedHost = "127.0.0.1", Port = 18080, Controls = true, Events = false,
        Mac = "02:11:22:33:44:55", Uuid = "12345678-1234-4234-9234-123456789012",
        Profiles = [new() { Stream = "mainStream", Codec = "H265", Width = 1920, Height = 1080, Fps = 20, Bitrate = 2048 }]
    };

    public class ControlProxy : DispatchProxy
    {
        public List<string> Calls { get; } = [];
        public int Writes { get; private set; }
        public int LastAiSensitivity { get; private set; } = 50;
        public bool SupportsPtz { get; set; } = true;
        public bool SupportsPrivacy { get; set; }
        public string? LastPtz { get; private set; }
        public bool FailNextPtzDirection { get; set; }
        private int _volume = 30;
        private static readonly CameraCapabilities Caps = new(null, new XElement("Support", new XElement("secret", "PRIVATE-SENTINEL")),
            new CameraFeatures(true, true, false, false, true, Siren: true));
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            string method = targetMethod!.Name; Calls.Add(method);
            if (targetMethod.ReturnType == typeof(Task))
            {
                Writes++;
                if (method == "SetVolumeAsync") _volume = (int)args![0]!;
                if (method == "SetAiSensitivityAsync") LastAiSensitivity = (int)args![1]!;
                if (method == "PtzAsync") LastPtz = (string)args![0]!;
                if (method == "PtzAsync" && LastPtz != "stop" && FailNextPtzDirection)
                { FailNextPtzDirection = false; return Task.FromException(new IOException("synthetic direction send failure")); }
                if (method == "SirenAsync") return Task.FromException(new IOException("PRIVATE-SENTINEL camera credential in failure"));
                return Task.CompletedTask;
            }
            if (targetMethod.ReturnType.IsGenericType && targetMethod.ReturnType.GetGenericTypeDefinition() == typeof(Task<>))
            {
                object? value = method switch
                {
                    "GetCapabilitiesAsync" => Caps with { Features = Caps.Features with { Ptz = SupportsPtz, Privacy = SupportsPrivacy } },
                    "GetPrivacyModeAsync" => SupportsPrivacy ? false : null,
                    "GetHttpFeaturesAsync" => new HttpFeatures(null, _volume, null, null, null, null, null,
                        MdSensitivity: 25, AiSensitivities: [new("people", LastAiSensitivity, 0)]),
                    "GetVolumeAsync" => _volume,
                    "GetServicePortsAsync" => (IReadOnlyList<ServicePortState>)new[] { new ServicePortState("https", 443, false) },
                    "GetMdSensitivityAsync" => 25,
                    "GetAiSensitivitiesAsync" => (IReadOnlyList<AiSensitivity>)new[] { new AiSensitivity("people", LastAiSensitivity, 0) },
                    "GetDetectionZoneAsync" => new DetectionZone("md", 2, 2, "0101"),
                    "GetLedStateAsync" => new XElement("LedState", new XElement("state", "auto"), new XElement("lightState", "off"),
                        new XElement("hidden", "PRIVATE-SENTINEL")),
                    _ => null
                };
                Type resultType = targetMethod.ReturnType.GenericTypeArguments[0];
                if (value == null && resultType.IsValueType && Nullable.GetUnderlyingType(resultType) == null)
                    value = Activator.CreateInstance(resultType);
                return typeof(Task).GetMethod(nameof(Task.FromResult))!.MakeGenericMethod(resultType).Invoke(null, [value]);
            }
            if (method == "ZoneTypes") return new[] { "md" };
            return targetMethod.ReturnType == typeof(bool) ? false : null;
        }
    }

    public class LiveReadProxy : DispatchProxy
    {
        public IBcCamera? Camera { get; set; }
        protected override object? Invoke(MethodInfo? method, object?[]? args) => method!.Name switch
        { "get_LiveCamera" => Camera, "get_Name" => "synthetic-live-source", _ => method.ReturnType == typeof(bool) ? false : null };
    }
    public class BcReadProxy : DispatchProxy
    {
        public List<uint> Commands { get; } = [];
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == "get_ChannelId") return (byte)0;
            if (method.Name != "SendCommandAsync") throw new Exception("Unexpected command in read-only diagnostic");
            Commands.Add((uint)args![0]!);
            var request = (BcXmlBody)args[1]!;
            if (request.Raw[0].Name.LocalName != "AiDetectCfg" || request.Raw[0].Element("type")?.Value != "people")
                throw new Exception("AI diagnostic used an incorrect source contract");
            var xml = new BcXmlBody(); xml.Raw.Add(XElement.Parse("<AiDetectCfg><chn>0</chn><type>people</type><sensitivity>75</sensitivity><stayTime>2</stayTime><token>PRIVATE-SENTINEL</token><scope><table>PRIVATE-SENTINEL</table></scope></AiDetectCfg>"));
            return Task.FromResult<BcMessage?>(new BcMessage { Meta = new BcMeta(), Xml = xml });
        }
    }
}
