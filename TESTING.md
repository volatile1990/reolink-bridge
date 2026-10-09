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

Der Pilot lief auf einer **Synology DS720+ mit DSM 7.2.1, Docker 24 und .NET 10**, mit eigener Macvlan-Adresse im Kameranetz und **UniFi Protect 7.3.70**.

| Kamera | Ergebnis |
|---|---|
| RLC-1212A | Direkte Baichuan-Anmeldung und H.265-Video mit 4512 × 2512 Pixeln erfolgreich. Als ONVIF-Kamera in Protect übernommen; Status `CONNECTED` mit Kompatibilitätsmodus `Improved`. Der erste Videoabschnitt des Protect-fMP4-Livestreams kam im gemessenen Versuch nach etwa **1,15 Sekunden**. |
| B1200 | Nach einer längeren NVR-Trennung erfolgreiche Nonce-Antworten aller drei Kit-Kameras. Eine Kamera mit bekanntem Passwort liefert direktes Baichuan-H.265-Video mit 4512 × 2512 Pixeln und wurde in Protect im Modus `Improved` aufgenommen. Zwei weitere Kameras weisen das übergebene Passwort mit Antwort 401 ab; ihre tatsächlichen Kamerapasswörter fehlen noch. |

Ein weiterer 45-Sekunden-Abruf durch Protect empfing rund 20,6 MB und 147 Videoabschnitte, mit etwa 0,34 Sekunden bis zum ersten Videoabschnitt. Der größte Abstand zwischen erkannten fMP4-Videoabschnitten betrug 2,83 Sekunden. Diese Messung unterscheidet noch nicht zwischen Quellverzögerung und der Paketierung in Protect; sie ist kein Beleg für ein perfekt flüssiges Bild.

Der B1200-Livestream wurde anschließend ebenfalls 45 Sekunden durch Protect abgerufen: erstes Video nach 0,154 Sekunden, 493 erkannte fMP4-Videoabschnitte, größter Abstand 0,184 Sekunden. Die Eingangsmetriken über rund 50 Sekunden zeigten 19,96 Frames/s, 10,45 Mbit/s codiertes Video und einen maximalen eingangsseitigen Frameabstand von 124 ms. Das bestätigt einen flüssigen begrenzten Pilotabschnitt; es ersetzt keinen längeren Betrieb und keinen Test anderer Kamerafirmwares.

## NVR-Sitzungen und verbleibende Voraussetzungen

Solange der Reolink-NVR verbunden war, schlossen die Kit-Kameras den zusätzlichen Baichuan-Login schon bei der Nonce-Anforderung. Ein kurzer Test 15 Sekunden nach der Trennung änderte das nicht. Nach 146 Sekunden ohne NVR-Verbindung antworteten alle drei Kameras; die Kamera mit bekanntem Passwort lieferte danach sofort Video. Das spricht für bestehende NVR-Sitzungen als Ursache der anfänglichen Abweisungen. Die genaue Begrenzung gleichzeitiger Sitzungen in der Firmware wurde nicht ermittelt.

Für einen eigenen Versuch den NVR vollständig vom Kameranetz trennen oder sauber ausschalten, die Kameras weiter mit Strom versorgen und bestehenden Sitzungen bis zu drei Minuten zum Ablaufen geben. Kein Factory Reset war für den erfolgreichen B1200-Pilot nötig. Die Bridge reicht danach einen Kamera-Stream an lokale Zuschauer weiter; der NVR sollte nicht parallel erneut die Kit-Kameras übernehmen.

Die Kamera-Zugangsdaten können vom NVR-Adminpasswort abweichen: Reolink dokumentiert zufällige Kamera-Passwörter durch Auto Add. Am NVR-Monitor lassen sie sich unter Kamera → Allgemein → Passwort ändern → Augensymbol nach NVR-Adminprüfung anzeigen, beziehungsweise unter Passwort erstellen/ändern ausdrücklich setzen. [Offizielle Reolink-Anleitung](https://support.reolink.com/articles/23994858026777-How-to-Configure-Camera-s-Password-via-Reolink-NVRs/).

Eine Daueraufzeichnung wurde für den B1200-Pilot angefordert, aber Protect lehnte sie mit `Should not set recordingMode to 'Always' due to lack of external HDD` ab. Die vorhandene interne SSD reicht auf dieser Console dafür nicht aus. Aufzeichnung und Wiedergabe sind deshalb noch nicht nachgewiesen; vor einer endgültigen NVR-Abschaltung muss geeigneter Protect-Aufnahmespeicher vorhanden sein.

Nach dem Versuch wurden die Pilot-Streams angehalten und der ursprüngliche NVR-Netzwerkanschluss wiederhergestellt. Alle sechs vorhandenen Kameras wurden danach am NVR wieder online gemeldet. Container, private Konfigurationen und die in Protect aufgenommenen Pilot-Geräte bleiben für die Fortsetzung vorbereitet.

## Herkunft

Die Upstream-Selbsttests und der übernommene Server stammen aus [Neolink.NET v1.1.0](UPSTREAM.md) von Oluwabori Olaleye. Die ursprünglichen Copyright-Hinweise und die [AGPL-3.0-Lizenz](LICENSE) bleiben erhalten; die zusätzlichen ONVIF-Vertragstests und der Headless-Pilot sind Erweiterungen dieser Bridge.
