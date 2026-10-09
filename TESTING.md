# Tests und Pilotstand

Stand: 9. Oktober 2026. Zum lokalen Ausführen wird das .NET-10-SDK benötigt; die folgenden Befehle laufen im Repository-Verzeichnis.

## Automatische Prüfungen

```sh
dotnet build src/Neolink.Server/Neolink.Server.csproj --configuration Release
dotnet run --project src/Neolink.Server/Neolink.Server.csproj --configuration Release --no-build -- selftest
dotnet run --project tests/Neolink.ProtectTests/Neolink.ProtectTests.csproj --configuration Release
```

Die letzten lokalen Läufe bestanden mit **159 Upstream-Selbsttests und 30 ONVIF-Vertragstests**. Der Release-Build hatte keine Fehler oder Warnungen.

Die zusätzlichen Vertragstests starten den tatsächlichen HTTP-Listener auf Loopback und verwenden synthetische Zugangsdaten und Medienzustände. Sie prüfen Geräteidentität und stabile UUID/MAC, Media1/Media2, ehrliche H.265-/H.264-Profile, RTSP-URLs, WSSE-/Basic-Anmeldung, Zeitabweichung und Replay-Schutz, Bereitschaft des Streams, XML-/XXE-Abweisung sowie die strenge Pilot-Konfiguration. Sie verbinden sich mit keiner echten Kamera. Details stehen im [Testprojekt](tests/Neolink.ProtectTests/README.md).

## Echter NAS-Pilot

Der Pilot lief auf einer **Synology DS720+ mit DSM 7.2.1, Docker 24 und .NET 10**, mit eigener Macvlan-Adresse im Kameranetz und **UniFi Protect 7.3.70**.

| Kamera | Ergebnis |
|---|---|
| RLC-1212A | Direkte Baichuan-Anmeldung und H.265-Video mit 4512 × 2512 Pixeln erfolgreich. Als ONVIF-Kamera in Protect übernommen; Status `CONNECTED` mit Kompatibilitätsmodus `Improved`. Der erste Videoabschnitt des Protect-fMP4-Livestreams kam im gemessenen Versuch nach etwa **1,15 Sekunden**. |
| B1200 | Die eigene Bridge wurde auf dem NAS gestartet, empfing aber kein Video; der Healthcheck lieferte korrekt HTTP 503. Alle drei getesteten Kit-Kameras schlossen die TCP-Verbindung bei der Nonce-Anforderung vor der Anmeldung. Auch mit vorübergehend vom Netzwerk getrenntem NVR blieb dieses Verhalten nach 15 Sekunden bestehen. Eine erfolgreiche direkte Anmeldung ist bisher nicht nachgewiesen. |

Ein weiterer 45-Sekunden-Abruf durch Protect empfing rund 20,6 MB und 147 Videoabschnitte, mit etwa 0,34 Sekunden bis zum ersten Videoabschnitt. Der größte Abstand zwischen erkannten fMP4-Videoabschnitten betrug 2,83 Sekunden. Diese Messung unterscheidet noch nicht zwischen Quellverzögerung und der Paketierung in Protect; sie ist kein Beleg für ein perfekt flüssiges Bild.

Der erfolgreiche RLC-1212A-Versuch bestätigt den Weg Kamera → Bridge → Protect. Dauerhafte Stabilität, Aufzeichnung und Wiedergabe sind noch nicht nachgewiesen. Für diese Aussagen müssen längere Livebild- und separate Aufnahmeprüfungen folgen. Die Ursache der B1200-Verbindungsrücksetzung ist noch nicht bestimmt; der kurze Trennungstest schließt länger bestehende Kamerasitzungen oder eine persistente NVR-Bindung nicht aus. Der NVR-Anschluss wurde anschließend wiederhergestellt und seine Erreichbarkeit geprüft.

## Herkunft

Die Upstream-Selbsttests und der übernommene Server stammen aus [Neolink.NET v1.1.0](UPSTREAM.md) von Oluwabori Olaleye. Die ursprünglichen Copyright-Hinweise und die [AGPL-3.0-Lizenz](LICENSE) bleiben erhalten; die zusätzlichen ONVIF-Vertragstests und der Headless-Pilot sind Erweiterungen dieser Bridge.
