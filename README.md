# Reolink Bridge für UniFi Protect

Eine kleine Headless-Bridge liest einen Reolink-Baichuan-Stream und stellt ihn als authentifiziertes RTSP und ONVIF bereit. Jede Bridge-Instanz verwendet genau eine Kamera und eine eigene Netzwerkadresse. Video wird weitergereicht; Auflösung, Codec und Bitrate der Kamera werden nicht verändert. Die Bridge zeichnet nicht auf und startet weder Weboberfläche noch MQTT.

Die Software basiert auf [Neolink.NET v1.1.0](UPSTREAM.md), benötigt .NET 10 und steht unter der [AGPL-3.0](LICENSE). In einem früheren NAS-Pilot wurden alle sechs Kameras separat in Protect aufgenommen: eine RLC-1212A und alle drei B1200 über vier Bridge-Instanzen, RLC-823A und E1 Zoom direkt über ONVIF. Ein paralleler 45-Sekunden-Liveabruf lief ohne Abruffehler; am gemeinsamen 100-Mbit/s-Switchzweig blieben jedoch größere Ankunftsabstände zu untersuchen.

Im aktuellen Test laufen **alle sechs Kameras über jeweils eine eigene Bridge**, einschließlich RLC-823A und E1 Zoom. Der Reolink-NVR ist physisch ausgeschaltet; Protect meldet die sechs Bridge-Geräte als verbunden mit dem Kompatibilitätsmodus `Improved`. Ein paralleler 60-Sekunden-Abruf aller sechs Livestreams lief ohne Abruffehler. Am gemeinsamen MokerLink-Switchzweig kommen Frames weiterhin schubweise an; eine anschließende passive TCP-Messung ergab jedoch keine Hinweise auf Paketverlust oder Empfangsstau. Daueraufzeichnung und Wiedergabe bleiben bis zum Einbau einer geeigneten Aufnahme-HDD in die Protect-Console offen; die interne SSD wurde dafür von Protect abgewiesen. Details und Messwerte stehen in [TESTING.md](TESTING.md).

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
- `onvif.profiles`: tatsächlichen Codec, Auflösung, Bilderrate und Bitrate in kbit/s eintragen. Die Beispieleinstellungen sind keine Transcodierung.

Die Config-Datei muss für die Container-UID/GID aus `.env` lesbar sein. Bei den Beispielwerten auf dem NAS:

```sh
sudo chown 1654:1654 secrets/bridge-config.json
chmod 600 secrets/bridge-config.json
```

Alternativ UID/GID in `.env` an den Besitzer der Datei anpassen. Passwörter stehen weder in Compose noch in Prozessargumenten. `secrets/`, lokale Compose-Dateien und `.env` sind von Git und vom Docker-Build ausgeschlossen.

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

Der Pilot bietet zunächst Video-Profile und Erkennung. Snapshots, Bewegungsereignisse, PTZ und Audio-Transcodierung sind keine zugesicherten Pilot-Funktionen. Der direkte Baichuan-Abruf aller drei B1200 und der RLC-1212A gelang im NAS-Pilot ohne den Reolink-NVR als Streamquelle. Die Übertragbarkeit auf andere Kamerafirmwares muss jeweils geprüft werden.

## Eingehenden Stream messen

`GET /metrics` am ONVIF-Port liefert die Messwerte des eingehenden Videostreams. Der Endpunkt verlangt HTTP Basic mit dem Bridge-Benutzer. Dieses Beispiel fragt das Passwort interaktiv ab:

```sh
curl --fail --user protect http://10.30.0.113:8080/metrics
```

| Messwert | Bedeutung |
| --- | --- |
| `incomingFrames` | Anzahl der eingegangenen Videoframes seit dem Start |
| `incomingVideoBytes` | Empfangene codierte Videobytes; Audio und Transport-Overhead sind ausgeschlossen |
| `lastVideoAgeMs` | Millisekunden seit dem zuletzt eingegangenen Videoframe |
| `maxArrivalGapMs` | Größter bisher abgeschlossener Abstand zwischen zwei eingegangenen Videoframes |

Die Zähler und der maximale Abstand gelten seit dem Prozessstart und schließen Wiederverbindungen ein. Ein Neustart setzt sie zurück; `maxArrivalGapMs` ist kein gleitendes Zeitfenster. Während einer laufenden Pause steigt zunächst `lastVideoAgeMs`; der maximale Abstand kann erst mit dem nächsten Frame aktualisiert werden. Die Abstände werden mit einer monotonen Uhr gemessen und hängen nicht von Änderungen der Systemzeit ab.

Für die tatsächliche Bilderrate und Bitrate zwei Messungen mit bekanntem Abstand `Δt` in Sekunden vergleichen: `FPS = ΔincomingFrames / Δt`, `Bitrate in bit/s = 8 × ΔincomingVideoBytes / Δt`. Für Mbit/s zusätzlich durch `1.000.000` teilen. Damit lässt sich ein stockender Kameraeingang von Problemen bei der Wiedergabe in Protect unterscheiden; die Werte messen den Eingang der Bridge.

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

[TESTING.md](TESTING.md) beschreibt die 35 ONVIF-Vertragstests, die Upstream-Selbsttests, den früheren Paralleltest mit vier Bridges und zwei direkten Kameras sowie den aktuellen Test mit sechs Bridges. Die Aufnahme-HDD bleibt Voraussetzung für Daueraufzeichnung.
