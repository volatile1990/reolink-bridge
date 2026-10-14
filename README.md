# Reolink Bridge für UniFi Protect

Eine kleine Headless-Bridge liest den Hauptstream und optional den echten Reolink-Substream und stellt sie als authentifiziertes RTSP und ONVIF bereit. Jede Bridge-Instanz verwendet genau eine Kamera, eine ONVIF-Identität und eine eigene Netzwerkadresse. Der Hauptstream wird unverändert weitergereicht. Ein optionaler kleiner Stream kann für Protect von H.264 nach H.265 konvertiert werden. Streaming verändert keine Kameraeinstellungen; ausdrücklich aktivierte Controls erlauben authentifizierte Änderungen mit Rücklesen. Die Bridge zeichnet nicht auf und startet weder Weboberfläche noch MQTT.

Die Software basiert auf [Neolink.NET v1.1.0](UPSTREAM.md), benötigt .NET 10 und steht unter der [AGPL-3.0](LICENSE). In einem früheren NAS-Pilot wurden alle sechs Kameras separat in Protect aufgenommen: eine RLC-1212A und alle drei B1200 über vier Bridge-Instanzen, RLC-823A und E1 Zoom direkt über ONVIF. Ein paralleler 45-Sekunden-Liveabruf lief ohne Abruffehler; am gemeinsamen 100-Mbit/s-Switchzweig blieben jedoch größere Ankunftsabstände zu untersuchen.

Der belegte Produktionsstand vom 14. Oktober 2026 verwendet **sechs eigene Bridges**, Protect `Improved`/GStreamer und Daueraufzeichnung. Alle sechs HQ- und fünf vorhandenen LQ-Aufnahmeenden schritten im lesenden Messfenster fort. Der frühere Pilot ohne geeignete Aufnahme-HDD ist historisch. Die hier weiterentwickelte Bridge **1.2.0** wurde zunächst offline getestet; daraus ergibt sich keine Aussage, dass diese neue Version bereits produktiv läuft. Details und datierte Messwerte stehen in [TESTING.md](TESTING.md).

Die [Dokumentation zu Zuverlässigkeit und Diagnose](docs/reliability.md) beschreibt Ressourcenbudgets, neue Stream-/Audio-Erkennung, PTZ/Talk-Recovery, optionale dauerhafte Ereignisse, Benutzerrechte, HTTPS-Pinning und reproduzierbare Quellstände.

## Konfiguration

Die Beispiele verwenden ein fiktives Netz `10.30.0.0/24`. Ersetze Adressen und Passwörter durch deine lokalen Werte. Die echte Konfiguration bleibt unter `secrets/` und wird im Container nur lesbar eingebunden.

```sh
cp compose.example.yaml compose.yaml
cp .env.example .env
mkdir -p secrets
cp examples/config.pilot.json secrets/bridge-config.json
```

Bearbeite `.env` und `secrets/bridge-config.json`:

- `BRIDGE_PARENT`: NAS-Netzwerkschnittstelle im Kameranetz. Bei einem bereits ungetaggten Anschluss die reale Schnittstelle verwenden; bei VLAN-Trunk die passende VLAN-Schnittstelle, zum Beispiel `eth0.30`.
- `BRIDGE_IP` und `onvif.advertised_host`: dieselbe freie, dauerhaft reservierte Adresse außerhalb des DHCP-Pools.
- `BRIDGE_MAC` und `onvif.mac`: dieselbe eindeutige MAC-Adresse. Die Beispieladresse beginnt mit `02` und ist lokal vergeben.
- `onvif.uuid`: pro virtueller Kamera einmal erzeugen und danach beibehalten, zum Beispiel mit `uuidgen`.
- `cameras[0].address`, `username`, `password`: Zugang zur echten Kamera auf Baichuan-Port `9000`.
- `users[0]`: separater Bridge-Benutzer für RTSP und ONVIF. Dieses Passwort wird bei der Aufnahme in Protect verwendet.
- `onvif.profiles`: `mainStream` zuerst, optional `subStream` danach; Codec, Auflösung, Bilderrate und Bitrate in kbit/s beschreiben die Ausgabe. Ohne ausdrücklich aktivierte Konvertierung müssen sie zur echten Kamera passen.
- `onvif.low_quality_transcode`: optional `"H265"`; konvertiert ausschließlich den kleinen H.264-Substream nach H.265, damit Protect bei einem H.265-Hauptstream beide Qualitäten auswählen kann.
- `onvif.low_quality_source_fps`: tatsächliche Bilderrate des nativen Substreams bei Konvertierung; Standard `20`. Die Bilderrate des zweiten Profils bestimmt die konvertierte Ausgabe.
- `onvif.low_quality_encoder`: ausdrücklich `"software"` (Standard, `libx265`) oder `"vaapi"` (`hevc_vaapi`). Bei einem Hardwarefehler erfolgt kein automatischer Wechsel auf Software.
- `onvif.low_quality_device`: DRM-Rendergerät für VAAPI; Standard `/dev/dri/renderD128`. Das Gerät und seine tatsächliche Gruppe müssen für den Containerbenutzer zugänglich sein.
- `onvif.low_quality_sample_aspect_ratio`: optional `"4:3"` für native H.264-HQ/LQ-Profile mit einem 4:3-Substream. Kennzeichnet ausschließlich LQ über H.264-SPS-VUI als 16:9; codierte Auflösung, Bilddaten, Audio und HQ bleiben erhalten. Das Empfangsgerät muss diese Kennzeichnung beachten.
- `onvif.gop_playout`: optionale Glättung und Normalisierung der Video-Zeitbasis; Standard `false`.
- `onvif.gop_audio`: optionales ursprüngliches AAC auf derselben geplanten GOP-Ausgabezeit; Standard `false`, benötigt `controls` und `gop_playout`.
- `onvif.playout_delay_ms`: zusätzliche Startreserve im GOP-Modus, in Millisekunden; Standard `0`, zulässig `0` bis `5000`.
- `onvif.events`: authentifizierte Kameraereignisse und `/events`-Replay; Standard `true`.
- `onvif.motion_event_policy`: `all` (Standard), `classified` oder `none`; steuert den gewöhnlichen ONVIF-Bewegungszustand, nicht die echten Klassen oder das Roh-Replay.
- `onvif.event_stale_seconds`: lokale Ablaufzeit für nicht mehr aktualisierte ONVIF-Zustände; Standard `120`, zulässig `5` bis `600`.

Mit `gop_playout: true` wartet die Bridge bis zum nächsten Schlüsselbild, verteilt gebündelte Bilder auf einzelne RTP-Bildgruppen und bildet die Kamera-Zeitbasis auf die gemessene Dauer der vollständigen Bildgruppe ab. Bei gebündelten Bildern oder auffälligen Kamera-Zeitstempeln verteilt sie die tatsächlich vorhandenen Bilder gleichmäßig über diese Dauer; bei plausiblen Zeitstempeln einzelner Bilder erhält sie deren relative Abstände. Danach sendet sie diese Bilder nach der normalisierten RTP-Zeitbasis. Codierte NAL-Inhalte, Auflösung und Codec bleiben erhalten; die ausgehenden RTP-Zeitstempel werden bewusst angepasst. Es werden keine zusätzlichen Bilder erzeugt. Ohne die gesonderte Option `gop_audio` bietet der Modus ausschließlich Video und erhöht die Live-Verzögerung um etwa eine Bildgruppe plus Startreserve. Eine Reserve von `2500` Millisekunden dient dem kontrollierten B1200-Versuch. Der normale Modus bleibt ohne diese ausdrückliche Konfiguration unverändert.

`gop_audio: true` bietet Audio erst nach tatsächlich beobachtetem, aktuellem AAC mit gültiger Codec-Konfiguration und frischem Audio-/Videoeingang an. Die zusätzliche Queue hält höchstens 512 AAC-Pakete beziehungsweise 512 KiB. AAC-Nutzdaten und die 1024-Sample-Schritte bleiben erhalten; Audio wird in den vorhandenen Video-Ausgabeplan einsortiert. Gemeinsame RTCP-Senderreports und CNAME ordnen beide RTP-Uhren derselben Ausgabezeit zu. Die Kamera liefert hier keinen AAC-Capture-PTS: die erste Zuordnung verwendet eine Ankunftsschätzung und belegt keine exakte ursprüngliche Aufnahmesynchronität. Authentifizierte `/metrics.gopAudio` meldet diese Grenze sowie Puffer, Verluste, Rücksetzungen und geschätzte Offsets. Es entsteht kein zusätzlicher Kamerastream oder Encoder; Low-Profil und Talk-Eingang behalten ihre bestehenden Pfade. Der produktive Ablauf benötigt einen begrenzten Canary mit echten Live-/Aufnahmedecodes und A/V-Zeitprüfung.

Die Config-Datei muss für die Container-UID/GID aus `.env` lesbar sein. Bei den Beispielwerten auf dem NAS:

```sh
sudo chown 1654:1654 secrets/bridge-config.json
chmod 600 secrets/bridge-config.json
```

Alternativ UID/GID in `.env` an den Besitzer der Datei anpassen. Passwörter stehen weder in Compose noch in Prozessargumenten. `secrets/`, lokale Compose-Dateien und `.env` sind von Git und vom Docker-Build ausgeschlossen.

## Zwei Qualitäten derselben Kamera

Protect verlangt für HQ und LQ denselben Videocodec. Bei H.264-Hauptstream und H.264-Substream werden beide nativ weitergereicht. Bei H.265-Hauptstream und H.264-Substream wird ausschließlich der kleine Stream mit ffmpeg nach H.265 konvertiert. Ein Beispielausschnitt für diesen Fall:

```json
{
  "low_quality_transcode": "H265",
  "low_quality_source_fps": 20,
  "profiles": [
    { "stream": "mainStream", "codec": "H265", "width": 4512, "height": 2512, "fps": 20, "bitrate": 8192 },
    { "stream": "subStream", "codec": "H265", "width": 896, "height": 512, "fps": 10, "bitrate": 512 }
  ]
}
```

Der Ausschnitt gehört in `onvif`; die tatsächlichen Auflösungen und nativen Bilderraten vorher prüfen. Das Laufzeitimage enthält ffmpeg. Die Softwarekonvertierung läuft mit begrenzten Encoder-Threads; der folgende Hardwaremodus verwendet die Intel-Grafik für das HEVC-Encoding. Das Endgerät muss H.265 weiterhin decodieren können, verarbeitet im Low-Modus aber erheblich weniger Bildpunkte. Fehlt H.265 grundsätzlich, löst der kleine H.265-Stream diese Codecgrenze nicht.

Beide Profile verwenden dieselbe Baichuan-Verbindung und Anmeldung an der Reolink-Kamera. Die getrennten Vorschau-Anforderungen werden anhand ihrer Nachrichtennummer zugeordnet, auch wenn ältere Kit-Kameras für beide denselben Streamtyp im Antwortheader melden. Es entsteht kein zweiter Container, keine zweite Adresse und keine weitere Protect-Kamera. MAC, UUID und Geräteidentität bleiben erhalten. Die Pfade `/Kameraname` und `/Kameraname/mainStream` bleiben Hauptstream; `/Kameraname/subStream` liefert die niedrige Qualität. Ereignisse und Snapshots verwenden ausschließlich die Hauptsitzung. Ein vorübergehend fehlender Substream wird auf derselben Anmeldung erneut angefordert und lässt den Hauptstream und dessen Healthcheck weiterlaufen; die Bereitschaft beider Qualitäten ist in `/metrics` einzeln sichtbar.

Nach dem Bereitstellen beider Profile die vorhandene Kamera in Protect über dessen HQ-/LQ-ONVIF-Profilwahl aktualisieren. Vorherige ausschließlich in HQ gespeicherte Aufnahmen erhalten dadurch keine nachträgliche LQ-Version. Die zusätzliche Aufnahme und Wiedergabe beider Qualitäten muss im vorhandenen Kameraobjekt geprüft werden.

Für den optionalen LQ-Anzeigeversuch bleibt ein 640 × 480-Bild bei derselben codierten Auflösung. `low_quality_sample_aspect_ratio: "4:3"` kennzeichnet seine Pixel als breiter, sodass sich ein Anzeigeverhältnis von 16:9 ergibt. Die Bridge passt nur die SPS-Metadaten an und startet keinen Encoder. Die Kennzeichnung gilt für sämtliche LQ-Verbraucher und neue LQ-Aufnahmen; eine Anwendung ausschließlich auf einen bestimmten Viewport ist damit nicht möglich. Der Schalter ist nur für native H.264-Haupt-/Subprofile und eine 4:3-Subauflösung zulässig. Beschädigte SPS sperrt allein die optionale LQ-Ausgabe bis zum nächsten gültigen Header; HQ, Audio und Ereignisse laufen weiter.

### Intel-VAAPI für den LQ-Stream

Für den Hardwaremodus im vorherigen `onvif`-Beispiel zusätzlich setzen:

```json
{
  "low_quality_encoder": "vaapi",
  "low_quality_device": "/dev/dri/renderD128"
}
```

`low_quality_transcode: "H265"` und zwei H.265-Ausgabeprofile bleiben erforderlich. Der Hauptstream wird weiter durchgereicht. FFmpeg decodiert den H.264-Substream und skaliert ihn auf der CPU; ausgewählte Bilder werden anschließend als NV12 auf die GPU übertragen und dort mit `hevc_vaapi` codiert. Das Noble-Laufzeitimage enthält FFmpeg und den Intel-Media-VAAPI-Treiber; die Bridge wählt dafür `LIBVA_DRIVER_NAME=iHD`.

Die LQ-Ausgabe benötigt gerade Breite und Höhe. Ihre Auflösung soll die native Substream-Auflösung nicht überschreiten, und ihre Bilderrate darf nicht höher sein als die tatsächlich gelieferte Quellrate. `low_quality_source_fps` beschreibt diese Quelle; der `fps`-Wert des zweiten Profils beschreibt die Ausgabe. Eine größere Ausgabe fügt keine Bilddetails hinzu, eine höhere Ausgabe als die Quelle keine zusätzlichen aufgenommenen Bilder. Der lokale Deployment-Helfer prüft diese Grenzen und nimmt für die vier geprüften Quellen maximal 896 × 512 Pixel an.

Bei gleicher konfigurierter Quell- und Ausgaberate ordnet die Bridge jedem codierten LQ-Bild die beobachtete Zeit seines Eingangsbildes zu. Liefert eine auf 20 fps konfigurierte Kamera bei Nacht tatsächlich 12,5 fps, behalten diese Bilder etwa 80 ms Abstand; sie werden nicht auf eine feste 50-ms-Uhr verkürzt. Eine begrenzte Queue mit 2048 Zeitstempeln verbindet Eingangs- und Ausgangsbilder außerhalb der rohen FFmpeg-Pipes. Die Dauer stammt aus monotonen Quellankünften, damit fehlerhafte Kamera-Uhrsprünge nicht durchgereicht werden. Gebündelte Bilder werden über den gemessenen Ankunftsabstand verteilt; die erste Bildgruppe verwendet die konfigurierte Rate als anfängliche Schätzung. Dies ersetzt keine Kamera-Capture-Zeit oder echte A/V-Synchronitätsmessung. Codec, Auflösung, Backend, Hauptstream und unveränderte AAC-Pakete behalten ihre Pfade. Unterschiedliche konfigurierte Quell-/Ausgaberaten verwenden weiterhin den bestehenden `fps`-Filter und dessen Ausgabeuhr.

Der Container benötigt nur das gewählte DRM-Rendergerät und dessen Gruppenzugriff. Auf dem geprüften DS720+-Host ist dies Gruppe `937`; auf anderen Hosts den tatsächlichen GID prüfen. Ergänzung des bestehenden Compose-Dienstes für diesen Host:

```yaml
devices:
  - /dev/dri/renderD128:/dev/dri/renderD128
group_add:
  - "937"
```

Die vorhandene Container-UID, das nur lesbare Root-Dateisystem, die Secrets-Bind-Mounts und die Netzkonfiguration bleiben erhalten. `vaapi` wird ausdrücklich gewählt: Ein fehlendes Gerät oder ein Encoderfehler lässt den LQ-Encoder mit begrenztem Backoff erneut versuchen. Er startet dabei keinen `libx265`-Ersatz. `/metrics.lowQualityEncoder` meldet Backend, Encoder, Zustand, Quell- und Ausgabefps, ausgegebene Bilder, Fehlerzahl und `softwareFallback: false`.

Im lokalen FFmpeg-6.1-/Intel-Gemini-Lake-Test verlor `hevc_vaapi` mit `-aud 1` decodierbare P-Bilder, obwohl Encoderstart und HEVC-Metadaten erfolgreich waren. Der Hardwarepfad verwendet deshalb `-aud 0`; Bildgrenzen werden zusätzlich aus `first_slice_segment_in_pic_flag` gelesen. Die Softwareausgabe behält ihre eigene AUD-Konfiguration. Die GPU-Regression zählt und decodiert sämtliche erwarteten Bilder einschließlich P-Bildern; ein erfolgreicher Prozessstart genügt nicht. Ausführung und NAS-Rollout sind in [TESTING.md](TESTING.md#hardware-lq-prüfung-und-rollout-am-12-oktober-2026) beschrieben.

## Pilot starten und prüfen

```sh
docker compose config --quiet
docker compose build
docker compose up -d
docker compose ps
docker compose exec camera-bridge curl --fail http://127.0.0.1:8080/health
```

Der Healthcheck meldet erst dann Erfolg, wenn Livevideo und dessen Codec-Metadaten vorliegen. Bleibt ein frischer Videoframe länger als fünf Sekunden aus, antwortet `/health` mit HTTP 503, auch wenn die Kamera noch als verbunden gilt. Er prüft keine Protect-Aufnahme. Ein fehlgeschlagener Healthcheck startet den Pilot nicht automatisch neu; das Beispiel verwendet `restart: "no"`.

Mit VLC von einem anderen Gerät im Netz den Stream öffnen:

```text
rtsp://10.30.0.113:8554/pilot/mainStream
```

Bei der Anmeldung den Bridge-Benutzer verwenden. Unter **Protect → Advanced Adoption** die virtuelle Adresse mit ONVIF-Port `10.30.0.113:8080` sowie dieselben Bridge-Zugangsdaten eintragen. Falls nötig, in Protect **Stream Compatibility Mode → Improved** wählen. Livebild und Aufnahme anschließend getrennt prüfen.

Für diesen Pilot müssen Protect und Bridge sich im Netz erreichen können; die Bridge benötigt außerdem TCP-Zugriff auf die echte Kamera an Port `9000`. RTSP läuft an `8554/TCP`, ONVIF an `8080/TCP` und lokale Erkennung an `3702/UDP`. Macvlan gibt dem Container eine eigene Adresse und MAC; der NAS-Host selbst erreicht seine Macvlan-Container standardmäßig nicht direkt. Der interne Healthcheck und Tests von Protect oder einem anderen LAN-Gerät funktionieren unabhängig davon.

Der Pilot bietet Video-Profile, Geräteerkennung, einen authentifizierten Snapshot-Endpunkt und Kameraereignisse. Er übernimmt Bewegung sowie vorhandene KI-Klassen über die bestehende Baichuan-Sitzung, ohne eigene Bildanalyse. Protect 7.3.70 verarbeitet die ONVIF-Bewegung, übernimmt diese Klassen-Topics aber nicht als native Smart-Detections; eine getrennte Integration muss dafür das geschützte Replay verwenden. Der historische Pilot bot noch keine PTZ-/Audio-Zusage. Die aktuelle optionale Controls-Erweiterung bietet reale PTZ-Fähigkeiten, ursprüngliches AAC und einen separaten Talkpfad; Modell-/Firmwareunterstützung muss weiterhin am Gerät geprüft werden. Der direkte Baichuan-Abruf aller drei B1200 und der RLC-1212A gelang im NAS-Pilot ohne den Reolink-NVR als Streamquelle. Die Übertragbarkeit auf andere Kamerafirmwares muss jeweils geprüft werden.

Die [Ereignisdokumentation](docs/onvif-events.md) beschreibt Anmeldung, Topic-Filter,
Replay-Cursor und die Bewegungsmodi. `classified` filtert gewöhnliche Bewegung
ohne echte Kameraklasse. `none` verhindert zusätzliche gewöhnliche
ONVIF-Bewegungsereignisse für eine bereits funktionierende getrennte
Klassenintegration; echte Klasseinträge, Klassenaustritte und das Roh-Replay
bleiben verfügbar.

## Vorschaubilder

ONVIF Media1 und Media2 liefern mit `GetSnapshotUri` eine stabile Adresse wie `http://10.30.0.113:8080/snapshot/main.jpg`. Der JPEG-Abruf verlangt den Bridge-Benutzer: Protect verwendet HTTP Digest mit MD5 und `qop=auth`; ein vorab gesendeter HTTP-Basic-Header wird ebenfalls akzeptiert. Die URI enthält keine Zugangsdaten. Digest bindet die Anmeldung an GET und den genauen Anfragepfad; kurzlebige Nonces und steigende Anfragenzähler verhindern die Wiederverwendung derselben Anmeldung.

Bereits aufgenommene Protect-Geräte müssen diese neue Snapshot-Adresse in ihren Kamerametadaten übernehmen. Im NAS-Pilot geschah das beim Wiederverbinden automatisch, ohne erneute Aufnahme der Kameras. Ein Browser-Reload allein ergänzt eine fehlende Adresse nicht. Nach einem zuvor fehlgeschlagenen Abruf kann Protect bis zu 60 Sekunden mit dem nächsten Versuch warten.

Die Bridge fordert über die bereits angemeldete Baichuan-Verbindung einen nativen Schnappschuss aus dem kleinen Stream an. Seine Auflösung kann vom Hauptvideo abweichen. Sie öffnet dafür keinen weiteren Kamerastream und benötigt keinen JPEG-Decoder. Gleichzeitige Anfragen teilen einen Abruf; dessen Ergebnis wird höchstens fünf Sekunden zwischengespeichert. Ein alter oder nicht bereiter Videoeingang sowie ein Wechsel der Kamerasitzung verwerfen den Cache. Die native Anforderung hat insgesamt drei Sekunden Zeit und eine Reassembly-Grenze von 4 MiB. Nicht unterstützte oder ungültige Antworten ergeben HTTP 503 mit einer kurzen Wiederholungspause. Ob die jeweilige Kamerafirmware den nativen Befehl unterstützt, muss am Gerät geprüft werden.

## Eingehenden Stream messen

`GET /metrics` am ONVIF-Port liefert die Messwerte des eingehenden Videostreams. Der Endpunkt verlangt HTTP Basic mit dem Bridge-Benutzer. Dieses Beispiel fragt das Passwort interaktiv ab:

```sh
curl --fail --user protect http://10.30.0.113:8080/metrics
```

| Messwert | Bedeutung |
| --- | --- |
| `incomingFrames` | Anzahl der eingegangenen Videobuffer seit dem Start; ein Buffer kann mehrere Bilder enthalten |
| `incomingVideoBytes` | Empfangene codierte Videobytes; Audio und Transport-Overhead sind ausgeschlossen |
| `lastVideoAgeMs` | Millisekunden seit dem zuletzt eingegangenen Videoframe |
| `maxArrivalGapMs` | Größter bisher abgeschlossener Abstand zwischen zwei eingegangenen Videoframes |
| `totalAccessUnits`, `multiAccessUnitBuffers`, `maxAccessUnitsPerBuffer` | Schätzung der Bilder anhand von VCL-Slice-Grenzen; Metadaten allein zählen nicht als Bild |
| `maxVideoBufferBytes` | Größter eingegangener codierter Videobuffer |
| `keyframeCount`, `keyframeAgeMs` | Anzahl und Alter der als Schlüsselbild markierten Buffer |
| `gopBytes`, `gopPackets`, `gopBuffered`, `gopCacheEvictions` | Aktueller Cache ab dem letzten Schlüsselbild und Anzahl verworfener Caches nach Überschreiten der Größenlimits |
| `lastCameraTimestampDeltaUs`, `maxCameraTimestampDeltaUs` | Letzte und größte vorwärts gerichtete Änderung der Kamera-Uhr, mit korrektem 32-Bit-Wrap |
| `cameraTimestampZeroDeltas`, `cameraTimestampBackwardCandidates` | Wiederholte Zeitstempel sowie mögliche Rücksprünge oder sehr große Vorwärtssprünge |

Die Zähler und der maximale Abstand gelten seit dem Prozessstart und schließen Wiederverbindungen ein. Ein Neustart setzt sie zurück; `maxArrivalGapMs` ist kein gleitendes Zeitfenster. Während einer laufenden Pause steigt zunächst `lastVideoAgeMs`; der maximale Abstand kann erst mit dem nächsten Frame aktualisiert werden. Die Abstände werden mit einer monotonen Uhr gemessen und hängen nicht von Änderungen der Systemzeit ab.

Auch die neuen Zähler und Maximalwerte gelten seit dem Prozessstart; die GOP-Größe und der GOP-Status beschreiben dagegen den aktuellen Cache. Kamera-Zeitstempel werden nicht über eine Wiederverbindung hinweg verglichen. Access-Unit-Zahlen sind eine Prüfung der codierten Struktur, keine erfolgreiche Bilddekodierung. Diese Diagnosen verändern weder die Buffer noch deren RTP-Zeitstempel.

Für die Bilderrate und Bitrate zwei Messungen mit bekanntem Abstand `Δt` in Sekunden vergleichen: `FPS ≈ ΔtotalAccessUnits / Δt`, `Bitrate in bit/s = 8 × ΔincomingVideoBytes / Δt`. `ΔincomingFrames / Δt` zählt dagegen eingegangene Buffer pro Sekunde; gebündelte Buffer können mehrere Bilder enthalten. Für Mbit/s zusätzlich durch `1.000.000` teilen. Damit lässt sich ein stockender Kameraeingang von Problemen bei der Wiedergabe in Protect unterscheiden. Beim Profil `sub` einer konvertierenden Bridge beziehen sich diese Zähler auf deren LQ-Ausgabe; unabhängige Dekodierung bestätigt zusätzlich die tatsächlich verwertbaren Bilder.

Im Headless-Pilot beendet eine ausdrücklich abgewiesene Kamera-Anmeldung (HTTP 401 in der zweiten Loginphase oder `AuthFailedException`) weitere Anmeldeversuche bis zum manuellen Neustart nach Korrektur der Zugangsdaten. Die RTSP- und ONVIF-Server bleiben erreichbar, `/health` liefert HTTP 503 und `/metrics` meldet `status: "authentication-failed"` sowie `authenticationFailed: true` am betroffenen Stream. Der Container bleibt laufen und gerät dadurch nicht in eine automatische Neustartschleife. Verbindungsfehler und Timeouts lösen weiterhin die üblichen Wiederverbindungsversuche aus.

## Stoppen und erweitern

```sh
docker compose down
```

Das entfernt den Pilot-Container und sein Compose-Netz. Kameraeinstellungen und Config-Datei bleiben erhalten. Für weitere Kameras jeweils eine eigene Container-Instanz mit eigener IP, MAC, UUID und Config-Datei anlegen. Erst nach erfolgreichem Pilot zusätzliche Instanzen oder automatischen Neustart aktivieren.

## Lokal bauen

```sh
dotnet build src/Neolink.Server/Neolink.Server.csproj --configuration Release
dotnet run --project src/Neolink.Server/Neolink.Server.csproj --configuration Release --no-build -- selftest
dotnet run --project tests/Neolink.ProtectTests/Neolink.ProtectTests.csproj --configuration Release
docker build -t reolink-bridge:pilot .
```

Der Container nutzt das .NET-10-SDK zum Bauen und das ASP.NET-10-Runtime-Image, da das Upstream-Projekt die ASP.NET-Frameworkreferenz enthält. Das Laufzeitimage arbeitet ohne Root-Rechte. Die GitHub-Actions-Prüfung baut und testet den Quellcode und baut das Containerimage; sie veröffentlicht und installiert nichts.

[TESTING.md](TESTING.md) beschreibt die ONVIF- und RTSP-Vertragstests, die Upstream-Selbsttests, den früheren Paralleltest mit vier Bridges und zwei direkten Kameras sowie den aktuellen Test mit sechs Bridges. Die Aufnahme-HDD bleibt Voraussetzung für Daueraufzeichnung.
