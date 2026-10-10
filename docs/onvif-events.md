# Kameraereignisse

Die Protect-Bridge ab diesem Stand empfängt Alarm-Pushes über dieselbe angemeldete
Baichuan-Verbindung wie das Video. Sie öffnet dafür keinen weiteren Kamerastream
und ändert keine Kamera-, Encoder- oder Erkennungsparameter. Ob die Kamera diese
Pushes und ihre KI-Klassen liefert, hängt von Modell, Firmware und deren
Erkennungseinstellungen ab.

`onvif.events` ist standardmäßig `true`; mit `false` bleibt die bisherige
Media-/Snapshot-Schnittstelle ohne Eventservice erhalten.
`onvif.event_stale_seconds` ist standardmäßig `120` und darf zwischen 5 und 600
liegen. Bei fehlenden Aktualisierungen oder einer verlorenen Videositzung räumt
die Bridge aktive ONVIF-Alarmzustände auf. Diese Aufräumaktionen sind keine
Kameraerkennungen und erscheinen nicht im Kamera-Replay.

`onvif.motion_event_policy` steuert ausschließlich den gewöhnlichen
ONVIF-Bewegungszustand:

- `all` (Standard) leitet jede echte Kameraaktivität einschließlich KI-Meldungen
  als Bewegung weiter.
- `classified` leitet Bewegung nur bei einer echten Kamera-Klassifizierung
  weiter. Unbekannte echte `AItype`-Tokens zählen ebenfalls; leere Tokens,
  `none`, `MD`, `motion` und Boolean-Platzhalter zählen nicht.
- `none` hält den gewöhnlichen ONVIF-Bewegungszustand immer auf `false`.
  Echte Menschen-, Fahrzeug- und Tierzustände bleiben als Klassen-Topics
  verfügbar, und das geschützte `/events`-Replay behält alle Kamerapushes.

`none` ist für eine getrennte Klassenintegration gedacht, die Ereignisse bereits
aus dem Replay übernimmt und zusätzliche gewöhnliche Bewegungsereignisse
vermeiden muss. Der Modus erzeugt selbst keine nativen Protect-Smart-Detections.
Bei fehlender Klassenintegration gehen diese Ereignismarkierungen in Protect
sonst verloren. Video und Snapshot-Verfügbarkeit werden davon nicht beeinflusst.

## ONVIF

`GetServices` und `GetCapabilities` melden `/onvif/events_service`. Unterstützt
sind `GetServiceCapabilities`, `GetEventProperties`,
`CreatePullPointSubscription`, `PullMessages`, `SetSynchronizationPoint`,
`Renew` und `Unsubscribe`. Die letzten vier Aufrufe verwenden den bei der
Subscription gelieferten, nicht erratbaren Endpunkt. Alle Eventaufrufe verlangen
den Bridge-Benutzer; eine Subscription gehört nur diesem Benutzer.
WSSE PasswordDigest mit Zeitschranke und Replay-Schutz sowie die bestehende
HTTP-Basic-Anmeldung werden unterstützt. SOAP-HTTP-Digest wird nicht angeboten.

Bewegung heißt `tns1:RuleEngine/CellMotionDetector/Motion`, mit
`Data/SimpleItem Name="IsMotion" Value="true"` oder `"false"`.
`VideoSourceConfigurationToken` stimmt mit dem Media-Profil überein.
Initialzustände und Synchronisation tragen `PropertyOperation="Initialized"`,
Zustandswechsel `"Changed"`. Eine reale KI-Meldung gilt auch dann als Bewegung,
wenn deren Baichuan-Status `none` lautet.

Tatsächlich beobachtete Menschen-, Fahrzeug- und Tierklassen werden zusätzlich
unter `tns1:RuleEngine/tnsre:ReolinkAI/tnsre:Person`, `Vehicle` oder `Animal`
angeboten. `tnsre` bezeichnet `urn:reolink-bridge:events:ai:1`, ausdrücklich eine
Bridge-Erweiterung. Diese Nachrichten enthalten `State` und `ObjectType`.
`GetEventProperties` nennt eine Klasse erst nach deren tatsächlicher Beobachtung.
Ohne KI-Klasse wird keine Klasse abgeleitet.

Der geprüfte ONVIF-Parser von Protect 7.3.70 verarbeitet diese Bewegungsnachrichten,
ignoriert aber die KI-Klassenthemen. Diese Implementierung erzeugt deshalb keine
nativen Person-/Tier-/Fahrzeug-SmartDetect-Filter in Protect. Ein separater Relay
kann die echten Klassen für benannte, protokollierte Alarm-Manager-Einträge
verwenden; Einrichtung und Verifikation dieses Relays erfolgen getrennt.

Topic-Filter unterstützen Concrete und ConcreteSet, einzelne Topics, deren
Vereinigung mit `|` und einen Unterbaum mit `//.`. Andere Filter werden
ausdrücklich abgelehnt. Grenzen pro Bridge: 8 Subscriptions, 256 Nachrichten je
Queue, 64 Nachrichten je Pull, 30 Sekunden maximale Wartezeit und 5 bis 3600
Sekunden Lease. Leases verwenden eine monotone Uhr. Ein Queueüberlauf ersetzt
die verlorene Historie durch aktuelle initialisierte Zustände, damit kein Alarm
wegen eines verlorenen Stopps dauerhaft aktiv bleibt.

## Geschütztes Kamera-Replay

`GET /events?after=0&limit=128` am ONVIF-Port liefert ausschließlich echte
Kamera-Pushes. HTTP Basic oder Digest mit dem Bridge-Benutzer ist erforderlich.
`after` ist eine nichtnegative, exklusive Sequenznummer; `limit` liegt zwischen
1 und 256 und ist standardmäßig 128. Der Ring enthält höchstens 256 Pushes.

```json
{
  "instanceId": "opaque-process-instance",
  "currentSeq": 2,
  "oldestSeq": 1,
  "truncated": false,
  "events": [
    {
      "seq": 1,
      "timestampUtc": "2026-10-10T00:00:00Z",
      "motion": true,
      "isActive": true,
      "classes": ["person"],
      "rawAiTypes": ["people"],
      "sourceStatus": "MD",
      "source": { "profile": "main", "channel": 0, "connectionEpoch": 0 },
      "zones": []
    }
  ]
}
```

`classes` enthält ausschließlich normalisierte `person`, `vehicle`, `animal` aus
dem Kamerapush; `rawAiTypes` behält auch andere echte Kameraklassen bei. Rohfelder
sind auf 16 Tokens mit jeweils 64 Zeichen und 64 Statuszeichen begrenzt;
Steuerzeichen werden entfernt. Zone-Indices werden vom bisherigen MotionPush-
Modell nicht geliefert, daher bleibt `zones` leer.
`source.connectionEpoch` ist eine monotone lokale Bridge-Generation, kein Wert
der Kamera. Bei einem Source-/Sitzungsreset steigt sie an, ohne einen künstlichen
Replay-Push einzufügen. Ein Relay kann so eine erneute echte Klassenerkennung nach
einer Wiederverbindung von einem wiederholten Push derselben Sitzung unterscheiden.

Ein Relay liest alle zurückgegebenen Ereignisse in Reihenfolge und merkt sich
die letzte tatsächlich gelesene `seq`, nicht blind `currentSeq` bei einem
begrenzten Batch. `truncated=true` kennzeichnet eine bereits verlorene Ringlücke.
Beim Prozessneustart wechselt `instanceId` und die Sequenz beginnt neu bei 0;
der Relay muss dann seinen Cursor zurücksetzen. Replay und beobachtete Klassen
sind bewusst auf den laufenden Prozess begrenzt. Es wird keine Aufnahme und
kein Video im Replay gespeichert.

`GET /metrics` ergänzt unter `events` unter anderem Push-, Übergangs- und
Zustellzähler, Queueüberläufe, Subscriptionzahl, aktive und beobachtete Klassen
sowie den Zeitpunkt des letzten echten Kamerapushs. Netzwerkadressen,
Kamerapasswörter und Anmeldedaten werden nicht in diesen Ereignisdaten ausgegeben.

Vor dem Rollout empfiehlt sich eine einzelne Kamera als Pilot. Anschließend
werden echter Start/Stop, AI-only Pushes, Protect-Motion-Ereignisse, kontinuierliche
Aufnahme, Snapshots und RTSP/GOP-Verhalten am laufenden System getrennt geprüft.
