# Tests und Pilotstand

Stand: 9. Oktober 2026. Zum lokalen Ausführen wird das .NET-10-SDK benötigt; die folgenden Befehle laufen im Repository-Verzeichnis.

## Automatische Prüfungen

```sh
dotnet build src/Neolink.Server/Neolink.Server.csproj --configuration Release
dotnet run --project src/Neolink.Server/Neolink.Server.csproj --configuration Release --no-build -- selftest
dotnet run --project tests/Neolink.ProtectTests/Neolink.ProtectTests.csproj --configuration Release
```

Die lokalen Prüfungen umfassen **159 Upstream-Selbsttests und 68 ONVIF-/RTSP-/Snapshot-Vertragstests**. Der Release-Build wird einschließlich der GOP-Ausgabe ohne Fehler oder Warnungen geprüft.

Die zusätzlichen Vertragstests starten den tatsächlichen HTTP-Listener auf Loopback und verwenden synthetische Zugangsdaten und Medienzustände. Sie prüfen Geräteidentität und stabile UUID/MAC, Media1/Media2, ehrliche H.265-/H.264-Profile, RTSP-URLs, WSSE-/Basic-Anmeldung, Zeitabweichung und Replay-Schutz, Bereitschaft des Streams, XML-/XXE-Abweisung sowie die strenge Pilot-Konfiguration. Sie verbinden sich mit keiner echten Kamera. Details stehen im [Testprojekt](tests/Neolink.ProtectTests/README.md).

Die GOP-Tests prüfen zusätzlich den tatsächlichen RTSP-TCP-Pfad: beide Mount-Aliase, einzelne RTP-Marker und Zeitstempel pro Bild, unveränderte codierte NAL-Inhalte auch nach FU-Fragmentierung, die unveränderte Standardausgabe sowie Audio-Abweisung im ausdrücklich aktivierten Video-Modus. Deterministische Uhren prüfen Drift, Wrap, begrenzte GOP-Puffer, Metadaten und Wiederaufnahme nach einer Lücke. PAUSE, TEARDOWN, Source-Wechsel und die echte DropOldest-Subscriberqueue sind ebenfalls abgedeckt. Die ergänzten Regressionen erhalten reale Bilder trotz eines zwölfsekündigen Raw-Clock-Sprungs, wiederholter oder rückläufiger RTP-Werte und gebündelter Access Units; sie prüfen zugleich plausible variable Kadenz und die Zuordnung des nächsten Schlüsselbilds.

Die Snapshot-Tests prüfen Media1/Media2-URIs, Anmeldung vor dem nativen Abruf, binäre JPEG-Ausgabe, Frische und Source-Wechsel, begrenzte Antwortgröße und Fristen. HTTP Digest wird mit echter Challenge-Verhandlung, Bindung an GET und Anfragepfad, Replay-Zählern, begrenztem Nonce-Cache und monotoner Ablaufzeit geprüft; vorab gesendetes Basic bleibt verfügbar. Der native Cache wird mit monotoner Uhr geprüft; parallele Aufrufer teilen eine Anforderung, deren Arbeit durch einen einzelnen abgebrochenen HTTP-Aufruf nicht beendet wird. Synthetische Baichuan-Peers prüfen den Befehl 109 einschließlich FullAES, Reassembly-Limits und verspäteter Antworten nach Abbruch. Nach dem vollständigen 201-Abschluss wird der binäre Snapshot-Modus freigegeben; ein später wiederverwendeter Nachrichtenzähler muss erneut eine XML-Bestätigung lesen können. Andere Videotransfers behalten ihren Modus. Diese Tests belegen noch keine Unterstützung durch eine konkrete Kamerafirmware.

## Echter NAS-Pilot

Der Pilot läuft auf einer **Synology DS720+ mit DSM 7.2.1, Docker 24 und .NET 10**, mit eigener Macvlan-Adresse je Bridge im Kameranetz und **UniFi Protect 7.3.70**. Die folgende frühere 45-Sekunden-Messung wurde mit Softwarestand `fd3d9b6` durchgeführt. Die lokalen Compose-Dateien, Geräteidentitäten und Zugangsdaten bleiben außerhalb des öffentlichen Repositorys.

### Native Vorschaubilder

Alle sechs NAS-Instanzen liefern mit Stand `648b7d7` native JPEGs über die bestehende Baichuan-Sitzung, ohne zweiten Videostream und ohne Bilddecoder auf dem NAS. Der begrenzte Snapshot-Befehl muss `streamType: sub` verwenden. Die vorherige Anforderung `subStream` führte an einer B1200 zu einem 1.947.116-Byte-Hauptstream-JPEG mit ungefähr 7,7 Sekunden Übertragungszeit und überschritt die dreisekündige Frist. Mit `sub` lieferte dieselbe Kamera ein rund 72-KB-JPEG innerhalb der Frist.

| Modell / Instanz | JPEG-Größe | Erster Abruf | Cache-Abruf |
|---|---:|---:|---:|
| RLC-1212A | 48.306 Byte | 357 ms | 6 ms |
| B1200, Instanz 1 | 57.113 Byte | 121 ms | 7 ms |
| B1200, Instanz 2 | 69.222 Byte | 158 ms | 5 ms |
| B1200, Instanz 3 | 71.581 Byte | 681 ms | 4 ms |
| E1 Zoom | 21.466 Byte | 682 ms | 8 ms |
| RLC-823A | 593.394 Byte | 1.200 ms | 18 ms |

Die parallele Prüfung bestätigte pro Instanz ONVIF `GetSnapshotUri` mit HTTP 200, authentifizierte JPEG-Ausgabe mit SOI/EOI-Markern und `image/jpeg`, anonyme Abweisung mit HTTP 401 sowie weiterhin `/health` mit HTTP 200. Alle sechs Protect-Geräte blieben `CONNECTED`, ohne `isPoorNetwork`; Protect übernahm die Snapshot-Adressen beim Wiederverbinden automatisch. Die Videoglättung mit 2.500 ms Reserve blieb für die drei betroffenen Instanzen aktiv.

Der abschließende Stand `c513d0d` ergänzt HTTP Digest, das der installierte Protect-Client benötigt. Ein Standard-Digest-Client erhielt bei allen sechs Bridges HTTP 200 mit gültigem JPEG. Anschließend lieferte auch **Protects eigener API-Endpunkt `/api/cameras/{id}/snapshot` für alle sechs Kameras HTTP 200 mit `image/jpeg` und vollständigen JPEG-Markern**. Die Bildgrößen lagen dort zwischen 28.443 und 83.229 Byte. Die Snapshot-Adressen wurden automatisch übernommen; ein manueller Patch oder erneutes Aufnehmen der Geräte war nicht erforderlich. Alle sechs Kameras meldeten weiterhin `CONNECTED`. Der Snapshot-Abruf braucht keine Aufnahme-HDD; diese bleibt für die getrennte Daueraufzeichnung erforderlich.

Protect speichert fehlgeschlagene Snapshot-Abrufe bis zu 60 Sekunden zwischen; sein Parameter `force=true` umgeht diesen Fehlercache nicht. Der vorherige leere HTTP-500-Abruf wurde erst nach Digest-Unterstützung erfolgreich. Vor der letzten Aktualisierung wurden die sechs Streameingänge zusätzlich 20 Sekunden beobachtet: jede Instanz beantwortete alle 20 Abfragen als `ready`, ohne Abruffehler. Diese Kurzprüfungen belegen den funktionierenden Vorschauabruf, keine mehrtägige Dauerprüfung.

### Aktiver Test: alle sechs Kameras über Bridges

Für den aktuellen Test ist der Reolink-NVR **physisch ausgeschaltet**. Auf dem NAS laufen sechs getrennte Bridge-Instanzen: RLC-1212A, drei B1200, E1 Zoom und RLC-823A. Protect meldet alle sechs Bridge-Geräte als `CONNECTED` mit `Improved`/GStreamer. Die bisherigen direkten Protect-Einbindungen von E1 Zoom und RLC-823A wurden vor dem Umzug privat gesichert und entfernt; es werden damit keine parallelen direkten Protect-Streams zu diesen beiden Kameras getestet.

Die neue **60-Sekunden-Messung** ist abgeschlossen. Für jede Bridge waren alle **61 von 61** parallelen `/metrics`-Abfragen erfolgreich und meldeten `ready`, ohne Fehler, Zähler-Reset oder Statuswechsel. Alle sechs Protect-WebSocket-Verbindungen antworteten mit HTTP `101`, lieferten Video ohne Abruffehler und erreichten die vollständige Abrufdauer von 60 Sekunden.

| Kamera | Streamquelle | Eingangs-FPS / Mbit/s | Max. beobachtetes Frame-Alter | Lifetime-Maximalabstand Start → Ende | Erstes Protect-Video / max. Abschnittsabstand |
|---|---|---|---|---|---|
| RLC-1212A | Bridge | 19,896 / 8,364 | 846,874 ms | 995,405 → 995,405 ms | 0,558 s / 1,020 s |
| B1200, Instanz 1 | Bridge | 19,829 / 10,436 | 59,694 ms | 182,118 → 182,118 ms | 0,371 s / 0,298 s |
| B1200, Instanz 2 | Bridge | 19,813 / 10,455 | 52,422 ms | 182,081 → 182,081 ms | 0,328 s / 0,316 s |
| B1200, Instanz 3 | Bridge | 17,366 / 9,232 | 976,118 ms | 2171,698 → 2171,698 ms | 1,104 s / 1,158 s |
| E1 Zoom | Bridge | 19,998 / 5,238 | 47,579 ms | 166,571 → 166,571 ms | 0,322 s / 0,219 s |
| RLC-823A | Bridge | 24,995 / 6,290 | 790,426 ms | 853,036 → 856,439 ms | 0,271 s / 1,017 s |

Die Messung fragte `/metrics` parallel einmal pro Sekunde ab. FPS und Videobitrate stammen aus Zählerdifferenzen; die Bitrate umfasst nur codiertes Video ohne Audio und Transport-Overhead. Das beobachtete Frame-Alter sind Stichprobenwerte; kürzere oder zwischen Abfragen liegende Pausen können unbemerkt bleiben. Lifetime-Maximalabstände seit Bridge-Start sind getrennt zu Beginn und Ende erfasst und sind keine Maximalwerte ausschließlich aus dem 60-Sekunden-Fenster. Protect-Abschnittsabstände messen ankommende fMP4-Fragmente, nicht einzelne Kamera-Frameabstände.

Am gemeinsamen MokerLink-Switchzweig kamen Frames schubweise an: Die ungefähr einsekündigen Zählerintervalle schwankten bei der RLC-1212A zwischen 3 und 36 FPS, bei B1200-Instanz 3 zwischen 1 und 32 FPS und bei der RLC-823A zwischen 6 und 45 FPS. Die übrigen drei Kameras lagen nahezu konstant bei 20 FPS. Das sind Ankunftsraten innerhalb der Intervalle, keine geänderten Kamera-FPS-Einstellungen; Werte über der eingestellten Rate passen zu nachgeholten Frames nach einer Pause.

Der kurze Test bestätigt den vollständigen Abruf aller sechs Bridges, aber noch keine perfekte Flüssigkeit oder langfristige Stabilität. Nach dem Wechsel von direkter Einbindung zur Bridge wurde das Livebild der RLC-1212A vom Nutzer als deutlich flüssiger beurteilt. Dieses visuelle Feedback ergänzt die Messung, ersetzt aber keinen längeren Stabilitätstest.

### Anschließende passive TCP-Messung

Eine weitere 60-Sekunden-Messung erfasste ausschließlich TCP-Header der drei Quellen am MokerLink-Zweig am NAS-VLAN-Interface. Sie öffnete keine zusätzliche Kameraquelle und speicherte weder Videodaten noch Paketdateien. Es wurden 191.820 Pakete erfasst, ohne vom Kernel gemeldete Capture-Verluste. In allen drei beobachteten Verbindungen gab es keine Sequenzüberschneidungen, wiederholten reinen ACK-Kandidaten oder Zero-Window-Pakete.

| Quelle | TCP-Nutzdaten / Mbit/s | Größter Abstand zwischen Nutzdatenpaketen |
|---|---|---|
| RLC-1212A | 8,470 | 44,368 ms |
| B1200, Instanz 3 | 9,376 | 4,013 ms |
| RLC-823A | 6,434 | 66,167 ms |

Damit ist in diesem Beobachtungsfenster kein Paketverlust oder Empfangsstau als Ursache bestätigt. TCP-Paketabstände und vollständige Video-Frames messen unterschiedliche Stufen. Die passive Messung lief nach der oben dokumentierten Frame-Messung; sie korreliert daher nicht exakt mit deren einzelnen Aussetzern. Offload, Capture-Grenzen und andere Betriebszeiten begrenzen die Aussage. Für verbleibende längere Frame-Pausen sind zeitgleich erfasste Transport- und Baichuan-/Parser-Metriken die nächste Eingrenzung; ein MokerLink-Hardwarefehler ist bislang nicht bewiesen.

### Eingrenzung der stockenden B1200, Instanz 3

Eine Diagnoseversion mit Softwarestand `6e7b1de` wurde ausschließlich auf B1200-Instanz 3 eingesetzt. Nach rund 979 Sekunden waren 16.886 Videobuffer und 17.123 geschätzte Access Units erfasst: 110 Buffer enthielten mehrere Bilder, der größte bis zu 16. Es gab keine GOP-Cache-Eviction. Die größten codierten Buffer überschritten 1 MB. Die anfängliche 92-Sekunden-Stichprobe hatte noch keine gebündelten Bilder erkannt; erst der längere Lauf bestätigte diesen Fall.

Eine zusätzliche 30-Sekunden-RTSP-Probe verwarf drei Sekunden Aufwärmphase. Sie empfing 474 vollständige RTP-Markergruppen ohne Sequenzunterbrechung oder unvollständige Gruppen. Über 26,968 Sekunden Empfangszeit liefen nur 23,901 Sekunden RTP-Zeit; der größte Ankunftsabstand betrug 645 ms vor einem Schlüsselbild mit rund 715 kB. Dessen RTP-Abstand zum vorigen Bild war nur rund 50 ms. Diese Probe misst Transport-Metadaten und dekodiert keine Bilder.

Die separat nachfolgende Protect-fMP4-Probe lief ebenfalls 30 Sekunden mit drei Sekunden Aufwärmphase und `useWallClock: false`. 241 gemessene Fragmente enthielten 477 Videoproben. Ihre Decode-Start-Zeitspanne betrug 26,777 Sekunden bei 26,688 Sekunden Empfangszeit; es gab keine Lücke oder Überlappung der Fragmentzeitbasis. Der maximale Fragment-Ankunftsabstand lag dennoch bei 749 ms. Diese spätere Beobachtung belegt eine annähernd zur Echtzeit passende Protect-Medienzeit, keine bestimmte interne Clockkorrektur und keine exakte Korrelation zum früheren RTP-Fenster.

Der optionale GOP-Ausgabemodus adressiert deshalb sowohl gebündelte Bilder als auch Ankunftspausen. Er hält vollständige Bildgruppen zurück, trennt deren Access Units, normalisiert den ausgehenden RTP-Clock auf die gemessene Gruppen-Dauer und sendet zeitlich geplant. Der Modus ist Video-only und standardmäßig ausgeschaltet. Pro GOP gelten 6 MiB, 900 Bilder und fünf Sekunden tatsächliche Ankunftsdauer als Grenzen; während der Ausgabe werden auch Daten des nächsten Schlüsselbilds gehalten. Das sind keine garantierten 6 MiB Gesamtprozessspeicher.

Der erste Versuch mit Softwarestand `6a29ff1` und 1500 ms Startreserve glättete die gewöhnlichen Abstände, verwarf bei einem auffälligen Kamera-Zeitsprung aber noch eine vollständige Bildgruppe. In einem parallelen 45-Sekunden-Abruf waren der mediane RTP-Ankunftsabstand 58 ms und der 95. Perzentilwert 88 ms; eine Wiederaufnahmepause betrug dennoch 3,18 Sekunden. Die Protect-fMP4-Probe sah entsprechend einen maximalen Fragmentabstand von 3,26 Sekunden. Der Nutzer meldete zudem einen dauerhaften Livebild-Hänger nach den ersten Sekunden. Dieser Versuch bestätigt deshalb keine verlässliche Glättung oder Wiedergabe.

Die anschließende Anpassung erhält gültige Bildgruppen auch bei auffälliger Kamera-Uhr: Sie verwendet deren tatsächliche Ankunftsdauer und Anzahl vorhandener Bilder für eine gleichmäßige Ausgabe. Gebündelte Bilder lösen ebenfalls diese Verteilung aus. Plausible relative Zeitstempel einzelner Bilder bleiben erhalten. Die Grenzen für echte Ankunftspausen, Epoch-Wechsel, Buffergrößen und Bildanzahl gelten weiterhin.

Softwarestand `c9626ec` wurde anschließend nur auf dieser Instanz mit 2500 ms Startreserve eingesetzt. Ein paralleler 45-Sekunden-Abruf lieferte nach Aufwärmphase 683 vollständige RTP-Bildgruppen mit 17,08 Gruppen/s, ohne Sequenzlücke oder unvollständige FU-Gruppe. Die RTP-Zeitspanne von 39,930 Sekunden entsprach der Empfangszeit. Der mediane Abstand betrug 57 ms, der 95. Perzentilwert 68 ms und das Maximum 124 ms. Protect lieferte 358 gemessene Fragmente mit 712 Videoproben; der maximale Fragmentabstand lag bei 171 ms, ohne Lücke oder Überlappung der Decode-Zeitbasis.

Die Quellmetriken nach 94 Sekunden Prozesslauf zählten neun gebündelte Buffer mit bis zu zwei Bildern, einen größten Ankunftsabstand von 1,48 Sekunden und einen größten Kamera-Zeitsprung von 8,67 Sekunden. Es gab keine Cache-Eviction. Diese Werte umfassen den gesamten Prozesslauf, nicht ausschließlich das 45-Sekunden-Probenfenster. Die gleichmäßige RTP- und fMP4-Lieferung bestätigt die Verbesserung in diesem begrenzten Versuch; reale Dekodierung und der erneute sichtbare Live-Test sind getrennte Prüfungen.

Der erneute Live-Test dieser B1200 wurde vom Nutzer anschließend als sehr gut beurteilt. Auf seinen Wunsch wurde derselbe geprüfte Softwarestand mit 2500 ms Reserve auch für RLC-1212A und RLC-823A aktiviert. UUIDs und Protect-Einbindungen blieben erhalten. Die anderen drei Bridges liefen weiter im Standardmodus.

Ein anschließender paralleler 45-Sekunden-Test der beiden zusätzlichen Instanzen ergab:

| Kamera | Gemessene RTP-Bildgruppen / FPS | Max. RTP-Bildabstand | Protect-Fragmente / Videoproben | Max. Protect-Fragmentabstand |
|---|---|---|---|---|
| RLC-1212A | 795 / 19,89 | 100 ms | 360 / 835 | 250 ms |
| RLC-823A | 1008 / 25,19 | 83 ms | 352 / 1056 | 192 ms |

Es gab keine RTP-Sequenzlücke oder unvollständige FU-Gruppe und keine Lücke oder Überlappung der Protect-Decode-Zeitbasis. Die Aufwärmphase betrug bei RTP fünf und bei Protect drei Sekunden. Fragmentabstände und Bildabstände messen unterschiedliche Stufen; eine genaue langfristige Verzögerungs- oder Flüssigkeitsgarantie lässt sich aus diesem begrenzten Versuch nicht ableiten.

Ein Softwaredecoder verarbeitete bei allen drei Kameras die vollständige 30-Sekunden-Mediendauer nach Null, ohne Stillstand. Nach Korrektur der feineren Zeitbasis des privaten Test-Muxers lieferte ein 15-Sekunden-Abruf 263, 302 und 380 decodierte Bilder für B1200-Instanz 3, RLC-1212A und RLC-823A, jeweils ohne Decoderwarnung. Diese Prüfungen speicherten keine Medien. Ein später gemeldeter Hänger der B1200 war in einem frischen Abruf nicht reproduzierbar: RTP und Protect lieferten weiter aktuelle Daten, und ein weiterer 10-Sekunden-Decode erzeugte 178 Bilder ohne Warnung. Der Nutzer bestätigte anschließend, dass mehrere Browser-Reloads die Wiedergabe wiederherstellten. Ein Browserproblem ist damit eine plausible Einordnung dieses letzten Vorfalls, keine abschließend bewiesene Ursache aller ursprünglichen Stream-Probleme.

### Früherer Pilot: vier Bridges und zwei direkte Kameras

Im früheren Pilot wurden alle sechs vorhandenen Kameras separat in Protect aufgenommen: vier über eigene Bridge-Instanzen auf dem NAS und zwei über ihre direkte ONVIF-Schnittstelle.

| Kamera | Ergebnis |
|---|---|
| RLC-1212A | Direkte Baichuan-Anmeldung und H.265-Video mit 4512 × 2512 Pixeln erfolgreich. Als ONVIF-Kamera in Protect übernommen; Status `CONNECTED` mit Kompatibilitätsmodus `Improved`. Der erste Videoabschnitt des Protect-fMP4-Livestreams kam im gemessenen Versuch nach etwa **1,15 Sekunden**. |
| B1200, drei Kameras | Alle drei Kit-Kameras liefern mit den korrekten Kamerazugangsdaten direktes Baichuan-Video über je eine eigene Bridge-Instanz. Jede wurde als separate ONVIF-Kamera in Protect aufgenommen. Der Reolink-NVR dient während dieses Tests nicht als Streamquelle. |
| RLC-823A und E1 Zoom | Beide eigenständigen Kameras sind zusätzlich über ihre direkte ONVIF-Schnittstelle separat in Protect aufgenommen. |

### Frühere Einzelmessungen

Ein weiterer 45-Sekunden-Abruf durch Protect empfing rund 20,6 MB und 147 Videoabschnitte, mit etwa 0,34 Sekunden bis zum ersten Videoabschnitt. Der größte Abstand zwischen erkannten fMP4-Videoabschnitten betrug 2,83 Sekunden. Diese Messung unterscheidet noch nicht zwischen Quellverzögerung und der Paketierung in Protect; sie ist kein Beleg für ein perfekt flüssiges Bild.

Der B1200-Livestream wurde anschließend ebenfalls 45 Sekunden durch Protect abgerufen: erstes Video nach 0,154 Sekunden, 493 erkannte fMP4-Videoabschnitte, größter Abstand 0,184 Sekunden. Die Eingangsmetriken über rund 50 Sekunden zeigten 19,96 Frames/s, 10,45 Mbit/s codiertes Video und einen maximalen eingangsseitigen Frameabstand von 124 ms. Das bestätigt einen flüssigen begrenzten Pilotabschnitt; es ersetzt keinen längeren Betrieb und keinen Test anderer Kamerafirmwares.

### Abschlussmessung des früheren Piloten

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

Nach der abgeschlossenen Messung des früheren Piloten wurden alle vier Bridge-Container bewusst gestoppt und endeten mit Exitcode `143`. Die ursprünglichen UniFi-Port-Overrides des NVR-Anschlusses wurden wiederhergestellt, einschließlich `port_security_enabled: false`. Der NVR war nach rund 45 Sekunden wieder erreichbar; `GetChannelstatus` bestätigte anschließend **alle sechs Kameras mit `online: 1`**. Diese damalige Wiederherstellung des NVR-Betriebs war bestätigt. Für den oben beschriebenen aktuellen Test wurde der NVR anschließend physisch ausgeschaltet und der Aufbau auf sechs aktive Bridges erweitert.

## Herkunft

Die Upstream-Selbsttests und der übernommene Server stammen aus [Neolink.NET v1.1.0](UPSTREAM.md) von Oluwabori Olaleye. Die ursprünglichen Copyright-Hinweise und die [AGPL-3.0-Lizenz](LICENSE) bleiben erhalten; die zusätzlichen ONVIF-Vertragstests und der Headless-Pilot sind Erweiterungen dieser Bridge.
