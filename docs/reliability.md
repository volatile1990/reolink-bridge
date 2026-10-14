# Zuverlässigkeit und Diagnose

Bridge 1.2.0 erhält HQ-/LQ-Profile, stabile Identitäten, Baichuan-Demultiplexing, originale AAC-Nutzdaten und die bestehenden GOP-/VAAPI-Ausgabepfade. Neue optionale Funktionen sind standardmäßig ausgeschaltet.

## Ressourcen und Sitzungswechsel

RTSP begrenzt Body und Gesamtheader auf je 64 KiB, Header auf 64, Verbindungen auf 128 global und 32 je Gegenstelle sowie Sitzungen auf acht je Verbindung. Teilanfragen haben eine 30-Sekunden-Frist. Ungenutzte Verbindungen laufen nach 120 Sekunden, ungenutzte Sitzungen nach 60 Sekunden ab. Erfolgreich gesendete Medien halten eine aktive Sitzung auch ohne zusätzlichen Keepalive offen.

Baichuan-Subscriptions und Hauptstream-Eingang halten höchstens 16 MiB und 1024 Nachrichten/Chunks, der Substream-Eingang höchstens 8 MiB. Ein Überlauf beendet die betroffene Preview für einen kontrollierten Neuaufbau. Einzelne rohe Medienchunks werden nicht verworfen. Die Hauptstream-Health bleibt bewusst unabhängig von optionalem LQ.

SDP verwendet einen aktuellen gemeinsamen Video-/Audio-Snapshot. Quellverlust verwirft alte Parameter; nach Reconnect wird Audio neu geprüft. PTZ-Discovery lädt tatsächliche Fähigkeiten. Native und REST-Bewegungen teilen denselben Besitzer; geordneter Shutdown versucht Stop vor dem Ende der Kamerasitzung. Vorübergehende Talkfehler erhalten begrenzten Backoff mit frischen Audioblöcken. Definitiv nicht unterstützte Funktionen werden nicht endlos wiederholt.

## Lesende Diagnose

Authentifizierte `/metrics` enthält Buildversion, Source-Epoch, aktuelle Audioinformationen, Eingangsqueuebudget/-alter, Alarmabonnement und feste bereinigte Encoderfehlerkategorien. Der zusätzliche Befehl verbindet die Teilzustände:

```console
python tools/doctor.py --local
python tools/doctor.py --bridge-url http://10.30.0.113:8080
```

Zugangsdaten werden interaktiv oder über den dokumentierten stdin-/Umgebungspfad gelesen; ein Passwortargument gibt es nicht. Der Befehl verwendet ausschließlich GET und löst keine Kamera- oder Wiedergabebefehle aus. Ein grüner Video-Healthcheck beweist keine Protect-Aufnahme oder akustische Synchronität.

## Optionale Ereignispersistenz

`onvif.event_replay_path` aktiviert einen privaten JSONL-Journalpfad für echte Kamerapushes. Ohne Opt-in bleibt das RAM-Replay mit 256 Einträgen. Persistenz hält Sequenz und Instanzidentität über Bridge-Neustarts; die Source-Epoch kennzeichnet trotzdem den Sitzungswechsel. Retention wird über `event_replay_retention_seconds` (Standard 86400, Bereich 60–604800) und `event_replay_max_bytes` (Standard 8 MiB, Bereich 64 KiB–64 MiB) begrenzt. Antworten bleiben paginiert. Ein unvollständiger letzter Append wird verworfen; beschädigte vollständige Datensätze werden verweigert. Schreibfehler erscheinen im Status.

Ein Linux-Container benötigt dafür ein eigenes beschreibbares Verzeichnis, etwa `/var/lib/reolink-bridge`, mit Containerbenutzer als Besitzer, Verzeichnismodus 0700 und Dateimodus 0600. Das übrige Root-Dateisystem und Secrets dürfen nur lesbar bleiben. `state/` wird von Git und Docker-Build ausgeschlossen. Ein Dateipfad wird niemals automatisch in bestehende Deployments eingetragen.

## Rechte und HTTPS

`onvif.control_users` begrenzt Aktorrechte optional auf eine Teilmenge vorhandener Bridge-Benutzer. Video-/Ereignisrechte bleiben erhalten; freigegebene Benutzer können Settings ändern, PTZ und Audio-Bedienpfade nutzen. Ein fehlender Wert erhält die bisherige Politik, eine leere Liste sperrt Aktoren. Native UDP-Talk bleibt zusätzlich an erlaubte Console-Adressen gebunden und benötigt mindestens einen Control-Benutzer.

`onvif.camera_https_certificate_sha256` bindet den HTTPS-Kamera-Client optional an einen exakten SHA256-Zertifikatfingerprint. Dafür muss eine ausdrückliche HTTPS-`http_address` konfiguriert sein. Ohne Pin bleibt die bisherige Self-Signed-Policy kompatibel. Zertifikatwechsel verlangen eine bewusste Aktualisierung des Pins.

## Reproduzierbare Quellen

```console
python tools/release_manifest.py --output artifacts/release.json --archive artifacts/source.zip
python tools/release_manifest.py --output artifacts/built-release.json --build --dotnet /path/to/dotnet
```

Das Manifest enthält Version, Gitrevision, Dirty-Zustand, tatsächlichen Quellhash und optional den Artefakthash. Der Build schreibt den Quellhash in die Informationsversion, schließt lokale private Konfiguration aus und verlangt ein frisches Ausgabeverzeichnis. Quellarchive schließen private Laufzeitdateien aus und haben deterministische Eintragsmetadaten. Containerbuilds übernehmen optional `BRIDGE_VERSION` und `BRIDGE_SOURCE_REVISION`. Bewegliche Basisimage-/Paketversionen werden durch den konkreten Imagedigest eines freigegebenen Builds dokumentiert.

Das mitgelieferte Compose-Beispiel bleibt ein Pilot mit `restart: no`; die bestehende produktive Installation verwendet `always` plus Watchdog. Diese Betriebsrichtlinien werden durch ein Quellupdate nicht automatisch verändert.
