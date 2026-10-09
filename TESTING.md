# Tests und Pilotstand

Stand: 9. Oktober 2026. Zum lokalen Ausführen wird das .NET-10-SDK benötigt; die folgenden Befehle laufen im Repository-Verzeichnis.

## Automatische Prüfungen

```sh
dotnet build src/Neolink.Server/Neolink.Server.csproj --configuration Release
dotnet run --project src/Neolink.Server/Neolink.Server.csproj --configuration Release --no-build -- selftest
dotnet run --project tests/Neolink.ProtectTests/Neolink.ProtectTests.csproj --configuration Release
```

Die letzten lokalen Läufe bestanden mit **159 Upstream-Selbsttests und 35 ONVIF-Vertragstests**. Der Release-Build hatte keine Fehler oder Warnungen.

Die zusätzlichen Vertragstests starten den tatsächlichen HTTP-Listener auf Loopback und verwenden synthetische Zugangsdaten und Medienzustände. Sie prüfen Geräteidentität und stabile UUID/MAC, Media1/Media2, ehrliche H.265-/H.264-Profile, RTSP-URLs, WSSE-/Basic-Anmeldung, Zeitabweichung und Replay-Schutz, Bereitschaft des Streams, XML-/XXE-Abweisung sowie die strenge Pilot-Konfiguration. Sie verbinden sich mit keiner echten Kamera. Details stehen im [Testprojekt](tests/Neolink.ProtectTests/README.md).

## Echter NAS-Pilot

Der Pilot läuft auf einer **Synology DS720+ mit DSM 7.2.1, Docker 24 und .NET 10**, mit eigener Macvlan-Adresse je Bridge im Kameranetz und **UniFi Protect 7.3.70**. Der aktuelle getestete Softwarestand ist `fd3d9b6`.

Alle sechs vorhandenen Kameras wurden separat in Protect aufgenommen: vier über eigene Bridge-Instanzen auf dem NAS und zwei über ihre direkte ONVIF-Schnittstelle. Die lokalen Compose-Dateien und Zugangsdaten bleiben außerhalb des öffentlichen Repositorys.

| Kamera | Ergebnis |
|---|---|
| RLC-1212A | Direkte Baichuan-Anmeldung und H.265-Video mit 4512 × 2512 Pixeln erfolgreich. Als ONVIF-Kamera in Protect übernommen; Status `CONNECTED` mit Kompatibilitätsmodus `Improved`. Der erste Videoabschnitt des Protect-fMP4-Livestreams kam im gemessenen Versuch nach etwa **1,15 Sekunden**. |
| B1200, drei Kameras | Alle drei Kit-Kameras liefern mit den korrekten Kamerazugangsdaten direktes Baichuan-Video über je eine eigene Bridge-Instanz. Jede wurde als separate ONVIF-Kamera in Protect aufgenommen. Der Reolink-NVR dient während dieses Tests nicht als Streamquelle. |
| RLC-823A und E1 Zoom | Beide eigenständigen Kameras sind zusätzlich über ihre direkte ONVIF-Schnittstelle separat in Protect aufgenommen. |

### Frühere Einzelmessungen

Ein weiterer 45-Sekunden-Abruf durch Protect empfing rund 20,6 MB und 147 Videoabschnitte, mit etwa 0,34 Sekunden bis zum ersten Videoabschnitt. Der größte Abstand zwischen erkannten fMP4-Videoabschnitten betrug 2,83 Sekunden. Diese Messung unterscheidet noch nicht zwischen Quellverzögerung und der Paketierung in Protect; sie ist kein Beleg für ein perfekt flüssiges Bild.

Der B1200-Livestream wurde anschließend ebenfalls 45 Sekunden durch Protect abgerufen: erstes Video nach 0,154 Sekunden, 493 erkannte fMP4-Videoabschnitte, größter Abstand 0,184 Sekunden. Die Eingangsmetriken über rund 50 Sekunden zeigten 19,96 Frames/s, 10,45 Mbit/s codiertes Video und einen maximalen eingangsseitigen Frameabstand von 124 ms. Das bestätigt einen flüssigen begrenzten Pilotabschnitt; es ersetzt keinen längeren Betrieb und keinen Test anderer Kamerafirmwares.

### Finale Messung mit allen sechs Kameras

Alle sechs Protect-fMP4-Livestreams wurden ohne verbundenen Reolink-NVR parallel für **45 Sekunden** abgerufen. Alle WebSocket-Verbindungen antworteten mit HTTP `101`, lieferten Video ohne Abruffehler und erreichten die vollständige Messdauer. Die vier Bridges meldeten jeweils H.265 mit 4512 × 2512 Pixeln.

| Kamera | Streamquelle | Eingangs-FPS / Mbit/s | Bisheriger max. Eingangsabstand | Erstes Protect-Video / max. Abschnittsabstand |
|---|---|---|---|---|
| RLC-1212A | Bridge | 12,88 / 9,00 | 3,3045 s | 0,743 s / 0,824 s |
| B1200, Instanz 1 | Bridge | 19,96 / 10,45 | 0,1317 s | 0,367 s / 0,341 s |
| B1200, Instanz 2 | Bridge | 19,94 / 10,44 | 0,1364 s | 0,388 s / 0,314 s |
| B1200, Instanz 3 | Bridge | 17,10 / 8,98 | 1,7873 s | 0,334 s / 1,682 s |
| RLC-823A | Direkte ONVIF-Kamera | Keine Bridge-Metriken | Keine Bridge-Metriken | 0,340 s / 0,726 s |
| E1 Zoom | Direkte ONVIF-Kamera | Keine Bridge-Metriken | Keine Bridge-Metriken | 0,362 s / 0,317 s |

Die Eingangs-FPS und -Bitraten stammen aus Zählerdifferenzen über **46,49 Sekunden** um diesen Abruf herum. Die Bitrate umfasst nur codiertes Video, ohne Audio und Transport-Overhead. Der bisherige maximale Eingangsabstand ist hingegen der Lifetime-Maximalwert seit dem jeweiligen Bridge-Start, einschließlich möglicher Wiederverbindungen; er ist **kein Maximalwert ausschließlich aus diesem 45-Sekunden-Fenster**. Protect-Abschnittsabstände messen ankommende fMP4-Fragmente und entsprechen nicht unmittelbar einzelnen Kamera-Frameabständen.

Die Messung belegt separate Einbindung, raschen Videoanfang und einen abgeschlossenen kurzen Parallelbetrieb. Sie belegt noch keinen perfekt flüssigen oder langfristig stabilen Betrieb. Am gemeinsamen 100-Mbit/s-Switchzweig bleiben insbesondere bei der RLC-1212A und B1200-Instanz 3 größere Abstände sichtbar; ihre Ursache wurde nicht abschließend bestimmt.

Bei der RLC-1212A meldete `GetIsp` anschließend `constantFrameRate: 0`, automatischen Tag-/Nachtmodus und automatische Belichtung. `GetEnc` meldete für den Hauptstream 20 FPS, 8192 kbit/s, GOP 2 und H.265 mit 4512 × 2512 Pixeln. Die Kamera erzwingt damit keine konstante Bilderrate; nächtlich reduzierte mittlere FPS sind eine plausible Erklärung für einen Teil der Abweichung von den eingestellten 20 FPS. Das erklärt noch nicht sämtliche längeren Ankunftsabstände und wurde nicht durch eine geänderte Kameraeinstellung getestet.

Ein zusätzlicher Versuch mit `max_encryption: aes` bei diesen beiden Kameras brach in der zweiten Login-Phase mit einer geschlossenen Verbindung ab und lieferte keine Frames. Ein Leistungsvergleich war deshalb nicht möglich. Die ursprünglichen Konfigurationen wurden wiederhergestellt.

## NVR-Sitzungen und verbleibende Voraussetzungen

Solange der Reolink-NVR verbunden war, schlossen die Kit-Kameras den zusätzlichen Baichuan-Login schon bei der Nonce-Anforderung. Ein kurzer Test 15 Sekunden nach der Trennung änderte das nicht. Nach 146 Sekunden ohne NVR-Verbindung antworteten alle drei Kameras; mit den korrekten Kamerazugangsdaten gelang schließlich auch der Videoabruf aller drei. Das spricht für bestehende NVR-Sitzungen als Ursache der anfänglichen Abweisungen. Die genaue Begrenzung gleichzeitiger Sitzungen in der Firmware wurde nicht ermittelt.

Für einen eigenen Versuch den NVR vollständig vom Kameranetz trennen oder sauber ausschalten, die Kameras weiter mit Strom versorgen und bestehenden Sitzungen bis zu drei Minuten zum Ablaufen geben. Kein Factory Reset war für den erfolgreichen B1200-Pilot nötig. Die Bridge reicht danach einen Kamera-Stream an lokale Zuschauer weiter; der NVR sollte nicht parallel erneut die Kit-Kameras übernehmen.

Die Kamera-Zugangsdaten können vom NVR-Adminpasswort abweichen: Reolink dokumentiert zufällige Kamera-Passwörter durch Auto Add. Am NVR-Monitor lassen sie sich unter Kamera → Allgemein → Passwort ändern → Augensymbol nach NVR-Adminprüfung anzeigen, beziehungsweise unter Passwort erstellen/ändern ausdrücklich setzen. [Offizielle Reolink-Anleitung](https://support.reolink.com/articles/23994858026777-How-to-Configure-Camera-s-Password-via-Reolink-NVRs/).

Eine Daueraufzeichnung wurde angefordert, aber Protect lehnte sie mit `Should not set recordingMode to 'Always' due to lack of external HDD` ab. Die vorhandene interne SSD reicht auf dieser Console dafür nicht aus. Die separate Einbindung der Kameras und Livevideo funktionieren; Daueraufzeichnung und Wiedergabe sind noch nicht nachgewiesen. Die verbleibende Speicherhürde ist eine geeignete zusätzliche Aufnahme-HDD im HDD-Schacht der Protect-Console, bevor der Reolink-NVR dauerhaft abgeschaltet wird.

Der Pilot ist nach der abgeschlossenen Messung **pausiert**. Alle vier Bridge-Container wurden bewusst gestoppt und endeten mit Exitcode `143`. Die ursprünglichen UniFi-Port-Overrides des NVR-Anschlusses wurden wiederhergestellt, einschließlich `port_security_enabled: false`. Der NVR war nach rund 45 Sekunden wieder erreichbar; `GetChannelstatus` bestätigte anschließend **alle sechs Kameras mit `online: 1`**. Die Wiederherstellung des NVR-Betriebs ist damit bestätigt. Container, private Konfigurationen und die in Protect aufgenommenen Geräte bleiben für die Fortsetzung nach Einbau der Aufnahme-HDD vorbereitet.

## Herkunft

Die Upstream-Selbsttests und der übernommene Server stammen aus [Neolink.NET v1.1.0](UPSTREAM.md) von Oluwabori Olaleye. Die ursprünglichen Copyright-Hinweise und die [AGPL-3.0-Lizenz](LICENSE) bleiben erhalten; die zusätzlichen ONVIF-Vertragstests und der Headless-Pilot sind Erweiterungen dieser Bridge.
