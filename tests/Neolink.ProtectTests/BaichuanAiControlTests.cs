using System.Reflection;
using System.Text.Json.Nodes;
using System.Xml.Linq;
using Neolink.Bc;
using Neolink.Bc.Xml;
using Neolink.Protocol;
using Neolink.Streaming;

internal static class BaichuanAiControlTests
{
    public static async Task RunAsync()
    {
        var ability = JsonNode.Parse("{\"abilityChn\":[{\"supportAiPeople\":{\"ver\":1},\"supportAiVehicle\":{\"ver\":1},\"supportAiDogCat\":{\"ver\":1},\"supportAiFace\":{\"ver\":0},\"aiTrack\":{\"ver\":1}}]}")!.AsObject();
        var state = JsonNode.Parse("{\"channel\":0,\"people\":{\"support\":1,\"alarm_state\":0},\"vehicle\":{\"support\":1},\"dog_cat\":{\"support\":1},\"face\":{\"support\":0}}")!.AsObject();
        Check(CameraControl.ParseHttpAiSupportedTypes(state, ability, 0).SetEquals(new[] { "people", "vehicle", "dog_cat" }), "Generic CGI AI response falsely advertised face/package");
        Check(CameraControl.SupportsAiTrack(ability, 0), "Firmware aiTrack capability was hidden by obsolete key");
        var legacyAbility = JsonNode.Parse("{\"abilityChn\":[{\"supportAITrack\":{\"ver\":1}}]}")!.AsObject();
        Check(CameraControl.SupportsAiTrack(legacyAbility, 0), "Legacy tracking capability regressed");
        var disabledTrack = JsonNode.Parse("{\"abilityChn\":[{\"aiTrack\":{\"ver\":0},\"supportAITrack\":{\"ver\":1}}]}")!.AsObject();
        Check(!CameraControl.SupportsAiTrack(disabledTrack, 0), "Legacy key overrode explicit modern tracking denial");
        var legacyState = JsonNode.Parse("{\"channel\":0,\"people\":0,\"vehicle\":1,\"face\":0}")!.AsObject();
        Check(!CameraControl.ParseHttpAiSupportedTypes(legacyState, null, 0).Contains("face"), "Legacy generic face state claimed support without hardware proof");
        bool badChannel = false; try { CameraControl.ParseHttpAiSupportedTypes(state, ability, 1); } catch (InvalidDataException) { badChannel = true; }
        Check(badChannel, "AI support accepted another camera channel");
        var source = XElement.Parse("<AiDetectCfg version='1.1' vendor='keep'><chn>0</chn><type>people</type><sensitivity>45</sensitivity><stayTime>0</stayTime><minTargetWidth>7.751900e-02</minTargetWidth><minTargetHeight>0.05</minTargetHeight><maxTargetWidth>0.5</maxTargetWidth><maxTargetHeight>1</maxTargetHeight><width>3</width><height>3</height><area>qv8S</area><vendorCfg><unknown rank='2'>keep</unknown></vendorCfg></AiDetectCfg>");
        string original = source.ToString(SaveOptions.DisableFormatting);
        var parsed = CameraControl.ParseBaichuanAiParameters("people", source);
        Check(parsed.Enabled == null && parsed.Fields.Count == 6, "BC parser invented an enable field or omitted actual fields");
        Check(parsed.Fields.Single(f => f.Id == "widthMin") is { Min: 0, Max: 1, Step: 0.000001, Writable: true }, "Target fraction confused with bitmap grid size");
        Check(CameraControl.ParseBaichuanAiZone("people", source) is { Cols: 3, Rows: 3, Table: "101010101" }, "MSB-first watched-bit area decoding failed");
        var updated = CameraControl.ApplyBaichuanAiParameters("people", source, new Dictionary<string, double> { ["stayTime"] = 3, ["widthMin"] = 0.123456 }, null);
        Check(updated.Element("minTargetWidth")?.Value == "1.234560e-01" && updated.Element("stayTime")?.Value == "3", "343 wire fraction format or delay incorrect");
        Check(updated.Element("area")?.Value == source.Element("area")?.Value && XNode.DeepEquals(updated.Element("vendorCfg"), source.Element("vendorCfg"))
            && updated.Attribute("vendor")?.Value == "keep" && source.ToString(SaveOptions.DisableFormatting) == original, "RMW lost unknown XML, mask or changed input");
        var zone = CameraControl.ApplyBaichuanAiZone("people", source, "000000000");
        byte[] oldBytes = Convert.FromBase64String(source.Element("area")!.Value), newBytes = Convert.FromBase64String(zone.Element("area")!.Value);
        Check(newBytes[0] == 0 && newBytes[1] == (oldBytes[1] & 0x7f) && newBytes[2] == oldBytes[2], "Area write lost final-byte padding or trailing camera bytes");
        Check(zone.Element("minTargetWidth")!.Value == source.Element("minTargetWidth")!.Value && XNode.DeepEquals(zone.Element("vendorCfg"), source.Element("vendorCfg")), "Zone update changed target size or vendor XML");
        var kit = new XElement(source); kit.Element("width")!.Value = "141"; kit.Element("height")!.Value = "78";
        var kitBytes = Enumerable.Repeat((byte)0xff, (141 * 78 + 7) / 8).ToArray(); kit.Element("area")!.Value = Convert.ToBase64String(kitBytes);
        Check(CameraControl.ParseBaichuanAiZone("people", kit) is { Cols: 141, Rows: 78 } kitZone && kitZone.Table.Length == 10998
            && CameraControl.ParseBaichuanAiParameters("people", kit).Fields.Single(f => f.Id == "widthMin").Max == 1,
            "Live Kit grid dimensions altered normalized target-size bounds");
        var changedKit = CameraControl.ApplyBaichuanAiZone("people", kit, new string('0', 10998));
        var changedKitBytes = Convert.FromBase64String(changedKit.Element("area")!.Value);
        Check(changedKitBytes.Length == 1375 && changedKitBytes[^1] == 3, "Non-byte-aligned Kit area lost its two padding bits");
        foreach (var bad in new Dictionary<string, double>[] { new() { ["widthMin"] = 0.7 }, new() { ["widthMax"] = 1.01 }, new() { ["stayTime"] = 9 },
            new() { ["stayTime"] = 0.5 }, new() { ["sensitivity"] = -1 }, new() { ["widthMin"] = double.NaN }, new() { ["width"] = 42 } })
            Reject(() => CameraControl.ApplyBaichuanAiParameters("people", source, bad, null), "Invalid BC field/range accepted");
        Reject(() => CameraControl.ApplyBaichuanAiParameters("vehicle", source, new Dictionary<string, double> { ["sensitivity"] = 20 }, null), "Wrong AI type accepted");
        Reject(() => CameraControl.ApplyBaichuanAiParameters("people", source, new Dictionary<string, double>(), true), "Nonexistent enable switch accepted");
        Reject(() => CameraControl.ApplyBaichuanAiZone("people", source, "111"), "Wrong bitmap dimensions accepted");
        Reject(() => CameraControl.ApplyBaichuanAiZone("people", source, "11111111x"), "Nonbinary area accepted");
        var duplicate = new XElement(source); duplicate.Add(new XElement("sensitivity", "40"));
        Reject(() => CameraControl.ApplyBaichuanAiParameters("people", duplicate, new Dictionary<string, double> { ["sensitivity"] = 20 }, null), "Ambiguous duplicate field accepted");
        var brokenMask = new XElement(source); brokenMask.Element("area")!.Value = "not base64!";
        Check(CameraControl.ParseBaichuanAiZone("people", brokenMask) == null, "Malformed mask was advertised");

        var bc = DispatchProxy.Create<IBcCamera, AiCameraProxy>(); var camera = (AiCameraProxy)(object)bc;
        camera.Source = source;
        var live = DispatchProxy.Create<ILiveCameraSource, ProtectCameraControlTests.LiveReadProxy>();
        ((ProtectCameraControlTests.LiveReadProxy)(object)live).Camera = bc;
        var control = new CameraControl(live);
        int unsupportedBefore = camera.Commands.Count;
        try { await control.SetAiSensitivityAsync("face", 20, CancellationToken.None); throw new Exception("Unreported face class accepted native write"); }
        catch (NotSupportedException) { }
        Check(camera.Commands.Count == unsupportedBefore, "Unsupported face guard touched source before capability proof");
        var types = await control.GetAiSensitivitiesAsync(CancellationToken.None);
        Check(types?.Select(t => t.Type).SequenceEqual(new[] { "people", "vehicle", "dog_cat" }) == true
            && camera.Commands.SequenceEqual(new uint[] { 342, 342, 342 }), "Read discovery fabricated types or did not reuse live connection");
        var nativeParams = await control.GetAiAlarmParametersAsync("dog_cat", CancellationToken.None);
        Check(nativeParams?.Type == "dog_cat" && nativeParams.Fields.Count == 6, "HTTP-less parameters omitted native fields");
        int before = camera.Commands.Count;
        await control.SetAiAlarmParametersAsync("vehicle", new Dictionary<string, double> { ["sensitivity"] = 65 }, null, CancellationToken.None);
        Check(camera.Commands.Skip(before).SequenceEqual(new uint[] { 342, 343, 342 }) && camera.LastWrite?.Element("type")?.Value == "vehicle"
            && camera.LastWrite?.Element("sensitivity")?.Value == "65" && camera.LastWrite?.Element("area")?.Value == source.Element("area")?.Value,
            "Write was not fresh-read/full-343 RMW of requested type");
        before = camera.Writes;
        try { await control.SetAiAlarmParametersAsync("people", new Dictionary<string, double> { ["stayTime"] = 10 }, null, CancellationToken.None); throw new Exception("Invalid direct delay write accepted"); }
        catch (ArgumentException) { }
        Check(camera.Writes == before, "Invalid value reached 343");
        camera.WrongType = true;
        try { await control.SetAiSensitivityAsync("people", 20, CancellationToken.None); throw new Exception("Wrong-type camera response accepted"); }
        catch (InvalidDataException) { }
        Check(camera.Writes == before, "Wrong-type response reached 343"); camera.WrongType = false;
        camera.RejectWrite = true;
        try { await control.SetDetectionZoneAsync("people", "111111111", CancellationToken.None); throw new Exception("Camera write rejection reported success"); }
        catch (CameraCommandException) { }
        Check(camera.LastWrite?.Element("area") != null, "Zone fallback did not use real source area");
        camera.RejectWrite = false; camera.NoWriteReply = true;
        try { await control.SetAiSensitivityAsync("people", 20, CancellationToken.None); throw new Exception("Unacknowledged write reported success"); }
        catch (IOException) { }
        camera.NoWriteReply = false; camera.IgnoreWrite = true;
        try { await control.SetAiSensitivityAsync("people", 80, CancellationToken.None); throw new Exception("Acknowledged but unapplied write reported success"); }
        catch (IOException) { }
        Console.WriteLine("Baichuan AI typed 342/343 RMW, scope, bitmap padding and rejection tests passed; synthetic source only.");
    }
    private static void Check(bool valid, string message) { if (!valid) throw new Exception(message); }
    private static void Reject(Action action, string message)
    { bool failed = false; try { action(); } catch (Exception e) when (e is ArgumentException or NotSupportedException) { failed = true; } Check(failed, message); }
    public class AiCameraProxy : DispatchProxy
    {
        public XElement Source { get; set; } = null!;
        public List<uint> Commands { get; } = [];
        public int Writes { get; private set; }
        public XElement? LastWrite { get; private set; }
        public bool WrongType { get; set; }
        public bool RejectWrite { get; set; }
        public bool NoWriteReply { get; set; }
        public bool IgnoreWrite { get; set; }
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == "get_ChannelId") return (byte)0;
            if (method.Name != "SendCommandAsync") throw new Exception("AI fallback attempted another camera session or actuator");
            uint id = (uint)args![0]!; Commands.Add(id);
            var request = (BcXmlBody)args[1]!; string type = request.Raw.Single().Element("type")!.Value;
            var extension = (ExtensionXml)args[2]!;
            Check(extension.ChannelId == 0 && id is 342 or 343, "Wrong Baichuan message/channel");
            if (id == 343)
            {
                LastWrite = new XElement(request.Raw.Single()); Writes++;
                if (RejectWrite) return Task.FromException<BcMessage?>(new CameraCommandException(343, 400));
                if (NoWriteReply) return Task.FromResult<BcMessage?>(null);
                if (!IgnoreWrite) Source = new XElement(LastWrite);
            }
            var cfg = new XElement(Source); cfg.Element("type")!.Value = WrongType ? "face" : type;
            var body = new BcXmlBody(); if (id == 342) body.Raw.Add(cfg);
            return Task.FromResult<BcMessage?>(new BcMessage { Meta = new BcMeta { ResponseCode = 200 }, Xml = body });
        }
    }
}
