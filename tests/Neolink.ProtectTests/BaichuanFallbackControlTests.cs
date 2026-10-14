using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json.Nodes;
using System.Xml.Linq;
using Neolink.Bc;
using Neolink.Bc.Xml;
using Neolink.Protocol;
using Neolink.Streaming;

internal static class BaichuanFallbackControlTests
{
    private static BcXmlBody Body(string xml) => BcXmlBody.TryParse(Encoding.UTF8.GetBytes(xml))!;
    private static BcXmlBody Copy(BcXmlBody body) => Body(Encoding.UTF8.GetString(body.Serialize()));
    private static void Check(bool value, string message) { if (!value) throw new Exception(message); }
    private static CameraControl Control(IBcCamera camera)
    {
        var live = DispatchProxy.Create<ILiveCameraSource, ProtectCameraControlTests.LiveReadProxy>();
        ((ProtectCameraControlTests.LiveReadProxy)(object)live).Camera = camera;
        return new CameraControl(live);
    }
    private static Dictionary<uint, BcXmlBody> Sources() => new()
    {
        [26] = Body("<body><VideoInput version='1.1' vendor='keep'><channelId>0</channelId><bright>80</bright><contrast>120</contrast><saturation>130</saturation><hue>128</hue><sharpen>100</sharpen><vendor>unchanged</vendor></VideoInput><InputAdvanceCfg version='1.1'><channelId>0</channelId><DayNight><mode>auto</mode><unknown>keep</unknown></DayNight><PowerLineFrequency><mode>50hz</mode><enable>1</enable></PowerLineFrequency><Exposure><mode>auto</mode><shutter>keep</shutter></Exposure></InputAdvanceCfg><vendorExtra rank='3'>keep</vendorExtra></body>"),
        [264] = Body("<body><audioCfg version='1.1'><channelId>0</channelId><volume>20</volume><talkAndReplyVolume>30</talkAndReplyVolume><vendor><audio>0</audio></vendor></audioCfg></body>"),
        [56] = Body("<body><Compression version='1.1'><channelId>0</channelId><mainStream><audio>1</audio><width>4512</width><height>2512</height><frame>20</frame><bitRate>8192</bitRate><videoEncType>1</videoEncType><gop>120</gop></mainStream><subStream><audio>1</audio><width>896</width><height>512</height><frame>20</frame><bitRate>1536</bitRate><videoEncType>0</videoEncType><gop>40</gop></subStream><vendor><audio>0</audio><opaque>keep</opaque></vendor></Compression></body>"),
        [46] = Body("<body><MD version='1.1'><channelId>0</channelId><enable>1</enable><width>3</width><height>2</height><sensInfoNew><sensitivityDefault>25</sensitivityDefault><sensInfoList><item><begin>0</begin><end>23</end><sensitivity>12</sensitivity></item></sensInfoList></sensInfoNew><sensitivityInfoList><item><sensitivity>43</sensitivity></item></sensitivityInfoList><scope><columns>4</columns><rows>3</rows><valueTable>8P8S</valueTable></scope><vendor>keep</vendor></MD></body>"),
        [44] = Body("<body><OsdDatetime version='1.1'><channelId>0</channelId><enable>1</enable><topLeftX>1</topLeftX><topLeftY>1</topLeftY><width>600</width><language>German</language></OsdDatetime><OsdChannelName version='1.1'><channelId>0</channelId><name>Synthetic</name><enable>1</enable><topLeftX>65537</topLeftX><topLeftY>65536</topLeftY><enWatermark>1</enWatermark><enBgcolor>0</enBgcolor></OsdChannelName><vendorExtra>keep</vendorExtra></body>")
    };

    public static async Task RunAsync()
    {
        var session = DispatchProxy.Create<IBcCamera, CameraProxy>(); var proxy = (CameraProxy)(object)session;
        proxy.Sources = Sources(); var control = Control(session);
        var image = await control.GetImageSettingsAsync(CancellationToken.None);
        Check(image is { Bright: 80, DayNight: "Auto", AntiFlicker: "50HZ", Flip: null, Mirror: null, Hdr: null }, "Native image fields/ranges fabricated or omitted");
        var features = await control.GetHttpFeaturesAsync(CancellationToken.None);
        Check(features is { Image.Bright: 80, Volume: 20, MdSensitivity: 25, Audio.RecordAudio: true, Audio.TalkVolume: 30, Audio.VisitorVolume: null, Osd.NamePos: "LowerMid" }, "Combined Protect snapshot omitted BC-only controls");
        var beforeImage = Copy(proxy.Sources[26]);
        int start = proxy.Commands.Count;
        await control.SetImageSettingsAsync(81, null, null, null, null, "Black&White", "Off", null, null, CancellationToken.None);
        Check(proxy.Commands.Skip(start).SequenceEqual(new uint[] { 26, 25, 26 }), "Image RMW was not read/write/readback on live session");
        var expectedImage = Copy(beforeImage); expectedImage.Raw[0].Element("bright")!.Value = "81";
        expectedImage.Raw[1].Element("DayNight")!.Element("mode")!.Value = "blackAndWhite";
        expectedImage.Raw[1].Element("PowerLineFrequency")!.Element("enable")!.Value = "0";
        Check(XNode.DeepEquals(XElement.Parse(Encoding.UTF8.GetString(expectedImage.Serialize())), XElement.Parse(Encoding.UTF8.GetString(proxy.LastWrite!.Serialize()))), "Image RMW dropped siblings/unknown fields");
        await control.SetVolumeAsync(40, CancellationToken.None);
        Check(proxy.LastWrite!.Raw[0].Element("volume")?.Value == "40" && proxy.LastWrite.Raw[0].Element("vendor")!.Element("audio")!.Value == "0", "Volume write changed unrelated config");
        await control.SetAudioVolumesAsync(55, null, CancellationToken.None);
        Check(proxy.LastWrite!.Raw[0].Element("talkAndReplyVolume")?.Value == "55" && proxy.LastWrite.Raw[0].Element("visitorVolume") == null, "Audio write invented absent visitor volume");
        var beforeEnc = Copy(proxy.Sources[56]);
        await control.SetRecordAudioAsync(false, CancellationToken.None);
        var expectedEnc = Copy(beforeEnc);
        foreach (var stream in expectedEnc.Raw[0].Elements().Where(e => e.Name.LocalName is "mainStream" or "subStream")) stream.Element("audio")!.Value = "0";
        Check(Encoding.UTF8.GetString(expectedEnc.Serialize()) == Encoding.UTF8.GetString(proxy.LastWrite!.Serialize()), "RecordAudio changed source FPS/GOP/codec/video settings or vendor audio");
        var beforeMd = Copy(proxy.Sources[46]);
        await control.SetMdSensitivityAsync(35, CancellationToken.None);
        var expectedMd = Copy(beforeMd); expectedMd.Raw[0].Element("sensInfoNew")!.Element("sensitivityDefault")!.Value = "35";
        Check(Encoding.UTF8.GetString(expectedMd.Serialize()) == Encoding.UTF8.GetString(proxy.LastWrite!.Serialize()), "MD sensitivity changed schedule, legacy inverse bands or zone");
        var zone = await control.GetDetectionZoneAsync("md", CancellationToken.None);
        Check(zone is { Cols: 3, Rows: 2, Table: "111000" }, "MD active dimensions or bitmap row stride incorrect");
        byte[] oldBits = Convert.FromBase64String(proxy.Sources[46].Raw[0].Element("scope")!.Element("valueTable")!.Value);
        await control.SetDetectionZoneAsync("md", "000111", CancellationToken.None);
        byte[] newBits = Convert.FromBase64String(proxy.LastWrite!.Raw[0].Element("scope")!.Element("valueTable")!.Value);
        Check(newBits[0] == ((oldBits[0] & 0x11) | 0x0e) && newBits.Skip(1).SequenceEqual(oldBits.Skip(1)), "MD zone write changed padded cells or trailing camera bytes");
        var osdBefore = Copy(proxy.Sources[44]);
        await control.SetOsdSettingsAsync(false, "LowerMid", true, "UpperRight", false, CancellationToken.None);
        var osd = proxy.LastWrite!;
        Check(osd.Raw.Count == osdBefore.Raw.Count && osd.Raw[1].Element("enWatermark")?.Value == "0" && osd.Raw[1].Element("topLeftX")?.Value == "65537"
            && osd.Raw[0].Element("topLeftX")?.Value == "65536" && osd.Raw[0].Element("language")?.Value == "German", "OSD siblings, normalized coordinates or unknown fields lost");
        int writes = proxy.Writes;
        await Reject(() => control.SetAudioVolumesAsync(null, 30, CancellationToken.None), "Absent audio field accepted");
        await Reject(() => control.SetImageSettingsAsync(256, null, null, null, null, null, null, null, null, CancellationToken.None), "Out-of-range image accepted");
        await Reject(() => control.SetMdSensitivityAsync(0, CancellationToken.None), "Out-of-range MD value accepted");
        await Reject(() => control.SetDetectionZoneAsync("md", "111x", CancellationToken.None), "Invalid MD mask accepted");
        Check(proxy.Writes == writes, "Invalid config reached physical write packet");
        proxy.IgnoreWrite = true; await Reject(() => control.SetVolumeAsync(99, CancellationToken.None), "Ignored write falsely confirmed"); proxy.IgnoreWrite = false;
        proxy.RejectWrite = true; await Reject(() => control.SetVolumeAsync(99, CancellationToken.None), "Camera refusal falsely confirmed"); proxy.RejectWrite = false;
        proxy.NoWriteReply = true; await Reject(() => control.SetVolumeAsync(99, CancellationToken.None), "Unacknowledged write falsely confirmed"); proxy.NoWriteReply = false;
        proxy.WrongChannel = true; writes = proxy.Writes;
        await Reject(() => control.SetVolumeAsync(50, CancellationToken.None), "Wrong-channel config written");
        Check(writes == proxy.Writes, "Wrong-channel response reached writer");
        var absent = Body("<body><OsdChannelName><enWatermark>1</enWatermark></OsdChannelName></body>");
        Check(CameraControl.ParseBcFallbackOsd(absent) is { CanShowName: false, CanShowTime: false, NamePos: null }, "Missing overlay fields were advertised as supported");
        var missingMode = Copy(proxy.Sources[26]); missingMode.Raw[1].Element("PowerLineFrequency")!.Element("mode")!.Remove();
        Check(CameraControl.ParseBcFallbackImage(missingMode)?.AntiFlicker == null, "Missing native mode created unsupported flicker choices");
        await CapturedCompressionContract();
        await HttpPreference();
        await WireRoundTrip();
        Console.WriteLine("Native BC image/audio/MD/OSD fallback tests passed; RMW, padding, readback and loopback packet contracts only.");
    }

    private static async Task CapturedCompressionContract()
    {
        // Exact non-secret GET56 payload from SDK models/B400/channels/0/enc-config.xml.
        var captured = Body("""
            <body><Compression version="1.1"><channelId>0</channelId><isNoTranslateFrame>1</isNoTranslateFrame>
            <mainStream><audio>1</audio><resolutionName>2560*1440</resolutionName><width>2560</width><height>1440</height><encoderType>cbr</encoderType><frame>25</frame><bitRate>6144</bitRate><encoderProfile>high</encoderProfile></mainStream>
            <subStream><audio>1</audio><resolutionName>640*360</resolutionName><width>640</width><height>360</height><encoderType>cbr</encoderType><frame>7</frame><bitRate>160</bitRate><encoderProfile>high</encoderProfile></subStream>
            <thirdStream><audio>0</audio><resolutionName></resolutionName><width>0</width><height>0</height><encoderType>vbr</encoderType><frame>0</frame><bitRate>0</bitRate><encoderProfile>default</encoderProfile></thirdStream>
            </Compression></body>
            """);
        var session = DispatchProxy.Create<IBcCamera, CameraProxy>(); var proxy = (CameraProxy)(object)session;
        proxy.Sources = new() { [56] = Copy(captured) }; var control = Control(session);
        Check((await control.GetAudioStateAsync(CancellationToken.None))?.RecordAudio == true, "Captured Compression was not recognized");
        await control.SetRecordAudioAsync(false, CancellationToken.None);
        var expected = Copy(captured);
        foreach (var stream in expected.Raw[0].Elements().Where(e => e.Name.LocalName.EndsWith("Stream", StringComparison.Ordinal))) stream.Element("audio")!.Value = "0";
        Check(Encoding.UTF8.GetString(proxy.LastWrite!.Serialize()) == Encoding.UTF8.GetString(expected.Serialize()), "Captured camera metadata changed while disabling record audio");
        await control.SetRecordAudioAsync(true, CancellationToken.None);
        Check(proxy.LastWrite!.Raw[0].Element("thirdStream")!.Element("audio")!.Value == "1"
            && proxy.LastWrite.Raw[0].Element("thirdStream")!.Element("frame")!.Value == "0", "Third-stream native audio flag omitted or video activated");
    }

    private static async Task HttpPreference()
    {
        var session = DispatchProxy.Create<IBcCamera, CameraProxy>(); var proxy = (CameraProxy)(object)session;
        proxy.Sources = Sources();
        using var api = new ReolinkHttpApi("http://127.0.0.1", "synthetic", "synthetic", 0);
        using var handler = new SyntheticHttpHandler(); using var client = new HttpClient(handler);
        typeof(ReolinkHttpApi).GetField("_http", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(api, client);
        var live = DispatchProxy.Create<ILiveCameraSource, ProtectCameraControlTests.LiveReadProxy>();
        ((ProtectCameraControlTests.LiveReadProxy)(object)live).Camera = session;
        var control = new CameraControl(live, httpApi: api);
        Check((await control.GetImageSettingsAsync(CancellationToken.None))?.Bright == 66, "Healthy HTTP image was replaced by BC source");
        await control.SetImageSettingsAsync(67, null, null, null, null, null, null, null, null, CancellationToken.None);
        Check(handler.ImageWrites == 1 && proxy.Commands.Count == 0, "Healthy HTTP source did not retain its native writer");
    }
    private sealed class SyntheticHttpHandler : HttpMessageHandler
    {
        public int ImageWrites;
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var input = JsonNode.Parse(await request.Content!.ReadAsStringAsync(ct))![0]!;
            string command = input["cmd"]!.GetValue<string>();
            JsonObject value = command switch
            {
                "Login" => new() { ["Token"] = new JsonObject { ["name"] = "synthetic-token", ["leaseTime"] = 3600 } },
                "GetImage" => new() { ["Image"] = new JsonObject { ["channel"] = 0, ["bright"] = 66 } },
                "GetIsp" => new() { ["Isp"] = new JsonObject { ["dayNight"] = "Color" } },
                "GetAbility" => new() { ["Ability"] = new JsonObject() },
                "SetImage" => Written(),
                _ => throw new Exception("Unexpected HTTP command")
            };
            JsonObject Written() { ImageWrites++; return new JsonObject(); }
            var output = new JsonArray(new JsonObject { ["cmd"] = command, ["code"] = 0, ["value"] = value, ["range"] = new JsonObject() });
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(output.ToJsonString(), Encoding.UTF8, "application/json") };
        }
    }
    private static async Task Reject(Func<Task> action, string message)
    {
        bool rejected = false;
        try { await action(); } catch (Exception error) when (error is ArgumentException or NotSupportedException or IOException or InvalidDataException or CameraCommandException) { rejected = true; }
        Check(rejected, message);
    }
    public class CameraProxy : DispatchProxy
    {
        public Dictionary<uint, BcXmlBody> Sources { get; set; } = [];
        public List<uint> Commands { get; } = [];
        public BcXmlBody? LastWrite { get; private set; }
        public int Writes { get; private set; }
        public bool IgnoreWrite { get; set; }
        public bool RejectWrite { get; set; }
        public bool NoWriteReply { get; set; }
        public bool WrongChannel { get; set; }
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == "get_ChannelId") return (byte)0;
            if (method.Name != "SendCommandAsync") throw new Exception("Fallback opened another session/login or actuator");
            uint id = (uint)args![0]!; Commands.Add(id);
            Check(((ExtensionXml)args[2]!).ChannelId == 0, "Native configuration request lacked channel extension");
            uint readId = id switch { 25 => 26, 265 => 264, 57 => 56, 47 => 46, 45 => 44, _ => id };
            if (readId != id)
            {
                LastWrite = Copy((BcXmlBody)args[1]!); Writes++;
                if (RejectWrite) return Task.FromException<BcMessage?>(new CameraCommandException(id, 400));
                if (NoWriteReply) return Task.FromResult<BcMessage?>(null);
                if (!IgnoreWrite) Sources[readId] = Copy(LastWrite);
            }
            if (!Sources.TryGetValue(readId, out var source)) return Task.FromException<BcMessage?>(new CameraCommandException(id, 400));
            var body = Copy(source);
            if (WrongChannel) body.Raw[0].Element("channelId")!.Value = "1";
            return Task.FromResult<BcMessage?>(new BcMessage { Meta = new BcMeta { MsgId = id, ResponseCode = 200 }, Xml = readId == id ? body : null });
        }
    }

    private static async Task WireRoundTrip()
    {
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
        var data = Sources(); var observed = new List<uint>();
        var server = Task.Run(async () =>
        {
            using var client = await listener.AcceptTcpClientAsync(stop.Token);
            var stream = client.GetStream(); var context = new BcContext(new EncryptionState());
            for (int i = 0; i < 4; i++)
            {
                var request = await BcCodec.ReadMessageAsync(stream, context, stop.Token); observed.Add(request.Meta.MsgId);
                Check(request.Extension?.ChannelId == 0 && request.Meta.MsgId is 26 or 25, "Real BC wire carried incorrect command/channel or an extra login");
                if (request.Meta.MsgId == 25) data[26] = Copy(request.Xml!);
                var response = new BcMessage { Meta = request.Meta with { ResponseCode = 200 }, Xml = request.Meta.MsgId == 26 ? Copy(data[26]) : null };
                await stream.WriteAsync(BcCodec.Serialize(response, context.Encryption), stop.Token);
            }
        }, stop.Token);
        try
        {
            await using var camera = await BcCamera.ConnectAsync("127.0.0.1", ((IPEndPoint)listener.LocalEndpoint).Port, 0, stop.Token);
            var control = Control(camera);
            Check((await control.GetImageSettingsAsync(stop.Token))?.Bright == 80, "Real BC response was not exposed through public getter");
            await control.SetImageSettingsAsync(82, null, null, null, null, null, null, null, null, stop.Token);
            await server;
            Check(observed.SequenceEqual(new uint[] { 26, 26, 25, 26 }), "Fallback command sequence opened another login, stream or connection");
            Check(data[26].Raw[0].Element("bright")?.Value == "82" && data[26].Raw.Count == 3, "Real BC framing altered XML/RMW siblings");
        }
        finally { stop.Cancel(); listener.Stop(); }
    }
}
