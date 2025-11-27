# Informationssystem zur Prognose von Bahnbeeinträchtigungen in Bremen

Autor: Jordan Guimfack Jeuna
Hochschule Bremerhaven – Studiengang Informationssysteme
Betreuer: Prof. Dr. Jens Hündling

**Kurzüberblick**
- Containerisierte Datenpipeline (Docker Compose) für Ingestion, Transformation und Analyse von Bahn‑ und Wetterdaten.
- Orchestrierung mit Apache Airflow; Speicherung und Modellierung in PostgreSQL; Observability über das Schema `metadata`.
- Schichtenarchitektur: `stg` (flüchtig, Rohdaten) → `psa` (persistente Kopie) → `dwh` (Modellierung und Konsum über Views).
- Stündliche Vertikalisierung der Open‑Meteo‑Payloads: 1 JSON‑Zeile pro Station × Stunde; Konsum über lesbare Views mit automatischer Duplikateliminierung.

**Inhalt**
- Architektur und Designentscheidungen
- Start und Umgebungsvariablen
- Verbindungen und Ports
- Schemata und Tabellen
- DAGs (DB‑Import, Open‑Meteo‑Forecast, Open‑Meteo‑Archive)
- Python‑Utilities (API‑Clients, DB‑Funktionen)
- Logik der Vertikalisierung und Views
- Checks und Troubleshooting
- Bekannte Grenzen und Verbesserungen

---

## Architektur und Designentscheidungen

### Datenschichten
Das System implementiert eine strikte Drei‑Schichten‑Architektur zur Trennung von Verantwortlichkeiten:

- **`stg` (Staging)**: Temporäre Aufnahme von Rohdaten im JSON‑Format
  - Hinweis: JSON‑basierte APIs (Stationen, Wetter) werden als `JSONB` gespeichert; XML‑basierte Timetables‑APIs werden unverändert als `TEXT` gespeichert.
  - Zweck: Schnelle Ingestion ohne Blockierung durch Transformationen
  - Lebensdauer: Flüchtig; Bereinigung unmittelbar nach erfolgreicher Kopie nach PSA
  - Begründung: Entkopplung von API‑Calls und Persistierung; Fehler in der Persistierung beeinträchtigen nicht die Ingestion

- **`psa` (Persistent Staging Area)**: Unveränderliche, dauerhafte Kopie aller Rohdaten
  - Zweck: Audit‑Trail und Grundlage für Reprocessing
  - Eigenschaften: Keine Transformationen, exakte 1:1‑Kopie aus STG (JSON‑APIs als `JSONB`, Timetables‑XML als `TEXT`)
  - Begründung: Ermöglicht vollständiges Replay bei Modellierungsfehlern ohne erneute API‑Calls; erfüllt Compliance‑Anforderungen

- **`dwh` (Data Warehouse)**: Modellierte, konsumierbare Datenstrukturen
  - Zweck: Analytische Speicherung und deklarative Views für Endnutzer
  - Eigenschaften: Vertikalisierte JSON‑Daten, dimensionale Verknüpfungen, automatische Duplikateliminierung
  - Begründung: Trennung von Rohdatenspeicherung und Konsumschicht; Views ermöglichen versionierbare, transparente Transformationen

- **`metadata`**: Vollständige Nachverfolgbarkeit aller Prozesse
  - Tabellen: `api_call_log` (API‑Requests), `process_log` (Pipeline‑Schritte)
  - Begründung: Observability, Debugging, Audit‑Trail, Performance‑Monitoring

### JSON‑Vertikalisierung: Von Arrays zu Zeilen

**Problem**: Open‑Meteo liefert Wetterdaten als parallele Arrays (`hourly.time`, `hourly.temperature_2m`, etc.), die schwer abfragbar und nicht normalisiert sind.

**Lösung**: Transformation in zeilenorientiertes Format
- Jeder Index `i` in `hourly.time` wird zu einer eigenständigen Zeile
- Alle korrespondierenden Messwerte am Index `i` werden in derselben Zeile gespeichert
- Speicherung als JSONB in `dwh.weather_forecast_vertical_raw` und `dwh.weather_history_vertical_raw`

**Begründung**:
- Standardisiertes, relationsfähiges Format
- Einfache SQL‑Abfragen auf Stundenbasis
- Flexibilität bei Schemaänderungen (neue Messungen erfordern keine DDL‑Änderungen)
- Balance zwischen Performance und Flexibilität

### DWH‑Views mit automatischer Duplikateliminierung

**Design**: Views (`dwh.v_weather_forecast_hourly`, `dwh.v_weather_history_hourly`) extrahieren strukturierte Daten aus JSONB und eliminieren automatisch Duplikate.

**Duplikateliminierung**: `DISTINCT ON (station_id, time)` mit `ORDER BY batch_id DESC`
- Pro Station und Zeitpunkt wird nur die Zeile mit dem höchsten (neuesten) `batch_id` zurückgegeben
- Garantiert duplikatfreie Ergebnisse für Konsumenten
- Alle historischen Batches bleiben in den Rohdatentabellen verfügbar

**Begründung der Architekturentscheidung**:
1. **Maximale Flexibilität**: Historische Daten bleiben vollständig erhalten für Audits und Analysen
2. **Einfache Wartung**: Duplikateliminierung ist deklarativ in der View‑Definition; keine komplexe Cleanup‑Logik
3. **Transparenz**: Konsumenten sehen automatisch nur aktuelle Daten; keine manuelle Filterung erforderlich
4. **Versionierung**: Views können einfach angepasst werden ohne Änderungen an Rohdaten
5. **Entkopplung**: Ingestion und Konsum sind vollständig getrennt

**Alternative Ansätze (bewusst verworfen)**:
- `UNIQUE` Constraint auf `(station_id, time)`: Würde Reprocessing blockieren; historische Batches gingen verloren
- `ON CONFLICT DO UPDATE`: Überschreibt alte Daten; Audit‑Trail unvollständig
- Cleanup‑Jobs: Zusätzliche Komplexität; Race‑Conditions möglich

**Trade‑off**: Potenzielle Performance‑Einbußen bei sehr großen Datenmengen
**Gegenmaßnahmen**: Indizes auf `(payload->>'station_id', payload->>'time', batch_id DESC)`; Option für materialisierte Views

### Nachverfolgbarkeit und Idempotenz

**Batch‑ID‑System**: Jeder Pipeline‑Lauf erhält eine eindeutige `batch_id` (Format: `source_YYYYMMDDHHmmss_uuid`)
- Propagierung durch alle Schichten: STG → PSA → DWH
- Verknüpfung mit `metadata.process_log` für vollständige Rückverfolgbarkeit

**Idempotenz‑Mechanismus**: `utils.db.is_batch_processed(process_name, batch_id)`
- Prüfung vor jeder Transformation: Wurde dieser Batch bereits erfolgreich verarbeitet?
- Bereits verarbeitete Batches: Status `skipped` in `metadata.process_log`; kein erneutes Einfügen
- Begründung: Wiederholte Airflow‑Retries oder manuelle Re‑Runs führen nicht zu Datenduplikaten

**Warum auf Transformationsebene, nicht auf Ingestion‑Ebene?**
- Ingestion soll so schnell wie möglich sein; Idempotenzprüfungen würden sie verlangsamen
- PSA enthält bewusst alle Batches (auch Duplikate aus API‑Retries) für vollständigen Audit‑Trail
- DWH‑Transformation ist der richtige Ort für Deduplication‑Logik

### ELT‑Ansatz mit JSONB

**Entscheidung**: Rohdaten von JSON‑basierten APIs als `JSONB` speichern; Extraktion über deklarative Views. XML‑basierte Timetables werden als `TEXT` unverändert gespeichert (keine JSON‑Umverpackung), um den API‑Return exakt zu erhalten.

**Vorteile**:
- **Flexibilität**: Neue Messungen von Open‑Meteo erfordern keine Schemaänderungen
- **Schnelle Ingestion**: Keine Transformationen während des API‑Calls
- **Versionierung**: View‑Definitionen sind versionierbar in Git; Änderungen sind nachvollziehbar
- **Transparenz**: Transformation ist deklarativ in SQL; keine versteckte Logik

**Nachteile (bewusst akzeptiert)**:
- Für JSONB: Geringfügig langsamere Abfragen als bei vollständig normalisierten Tabellen; View‑Performance kann bei sehr großen Datenmengen leiden
- Für Timetables‑TEXT: Keine unmittelbare JSON‑Verarbeitung in SQL; eventuelle spätere Parsing‑Schritte erfolgen explizit in separaten Transformationen

**Gegenmaßnahmen**:
- Indizes auf JSONB‑Attributen (`payload->>'time'`, `payload->>'station_id'`)
- Option für materialisierte Views bei Performance‑Problemen

### Transformation innerhalb der Import‑DAGs

**Entscheidung**: Kein separater Transform‑DAG; Vertikalisierung erfolgt am Ende der Import‑DAGs (`transform_forecast_to_dwh`, `transform_history_to_dwh`)

**Begründung**:
- **Geringere Latenz**: Daten sind unmittelbar nach Ingestion konsumierbar
- **Einfachere Koordination**: Keine DAG‑Abhängigkeiten oder Sensoren erforderlich
- **Klarer Prozessfluss**: Linearer Ablauf von API‑Call bis DWH‑Speicherung
- **Atomarität**: Ein Lauf = ein Batch von der Ingestion bis zur Transformation

**Trade‑off**: Geringere Entkopplung zwischen Ingestion und Transformation
**Gegenmaßnahmen**: Idempotenz‑Mechanismus und umfassendes Prozess‑Logging garantieren Datenintegrität

**Alternative (bewusst verworfen)**: Separater Transform‑DAG mit Sensoren
- Würde zusätzliche Latenz einführen
- Komplexere Fehlerbehandlung bei Sensor‑Timeouts
- Höherer Koordinationsaufwand

### Batching und API‑Laststeuerung

**Forecast**: Koordinaten werden in Batches von maximal 200 Stationen gruppiert
- Begründung: URL‑Längenbeschränkungen; Lastverteilung auf Open‑Meteo‑API
- Effekt: Stabile Request‑Größen; bessere Fehler‑Isolation (ein fehlerhafter Batch blockiert nicht alle Stationen)

**Archive**: Startfenster über `ARCHIVE_START_DATE`; Enddatum automatisch J‑2
- Begründung: Kontrollierte historische Verarbeitung ohne Überlastung
- Flexibilität: Startdatum konfigurierbar für Backfills/Tests; Enddatum wird automatisch als J‑2 (heute minus zwei Tage) gesetzt
 - Fenster rollierend: Erstlauf verwendet `ARCHIVE_START_DATE` (z. B. `2025‑01‑01`); Folgeläufe setzen `start_date = letztes end_date` aus `metadata.api_call_log`, `end_date = heute‑2 Tage`

### Scheduling und Catchup

**Forecast**: `0 */6 * * *`, `catchup=false`
- Läuft alle 6 Stunden (00:00/06:00/12:00/18:00) für zeitnahe Prognoseupdates

**Archive**: `0 3 * * *`, `catchup=false`
- Standardfenster J‑2 (Open‑Meteo aktualisiert Historie mit 1–2 Tagen Verzögerung)
- Initial Load: Manuell mit `ARCHIVE_START_DATE` (Start); Enddatum ist immer „heute‑2 Tage“ (J‑2) und wird vom DAG berechnet; alternativ temporär `catchup=true`
 - Hinweis: Fenster rollierend zwischen Läufen (siehe Abschnitt „Archive“ oben)

---

## Schnellstart

- `.env` aus `.env.example` erstellen und Variablen setzen
- Start: `docker compose up -d --build`
- Airflow Web UI: `http://localhost:8085` (Login `admin` / `admin`)
- pgAdmin: `http://localhost:8081` (Admin: `admin@local.test` / `admin`)

---

## Umgebungsvariablen (wichtigste)

- **API DB**: `DB_CLIENT_ID`, `DB_API_KEY`
- **Airflow**: `AIRFLOW_FERNET_KEY`, `AIRFLOW_WEBSERVER_SECRET_KEY` (identisch für Webserver/Scheduler)
- **Datenbank**: `DATA_DB_HOST`, `DATA_DB_PORT`, `DATA_DB_NAME`, `DATA_DB_USER`, `DATA_DB_PASSWORD`
- **Postgres (Container)**: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- **Open‑Meteo Archive**: `ARCHIVE_START_DATE`, `OPEN_METEO_ARCHIVE_HOURLY` (Enddatum J‑2 automatisch vom DAG)
  - Hinweis: Erstlauf nutzt `ARCHIVE_START_DATE`; Folgeläufe setzen `start_date = letztes end_date` aus dem API‑Log

- **Timetables Filter (Bremen)**:
  - `DB_TIMETABLES_FILTER_NAME_PREFIX` (Standard: `Brem%`)
  - `DB_TIMETABLES_FILTER_DS100_PREFIX` (Standard: `HB%`)
  - `DB_TIMETABLES_STATIONS_LIMIT` (optional: Limitierung der Anzahl EVA)

---

## PostgreSQL‑Verbindungen

- **Lokaler Client**: `psql -h localhost -p 5433 -U dw -d train_dw` (Credentials: `dw/dw`)
- **pgAdmin**: Server anlegen mit `Host=postgres`, `Port=5432`, `User/Pass=postgres/postgres` oder `dw/dw`

---

## Schemata und Tabellen (wichtigste)

**Metadata**:
- `metadata.api_call_log`: Vollständige API‑Request‑Historie
- `metadata.process_log`: Pipeline‑Schritte mit Start/Ende/Status

**Staging**:
- `stg.db_stations_raw`, `stg.weather_forecast_raw`, `stg.weather_history_raw`
 - Timetables (XML, `payload TEXT`): `stg.timetables_plan_raw`, `stg.timetables_fchg_raw`, `stg.timetables_rchg_raw`

**Persistent Staging Area**:
- `psa.db_stations_raw`, `psa.weather_forecast_raw`, `psa.weather_history_raw`
 - Timetables (XML, `payload TEXT`): `psa.timetables_plan_raw`, `psa.timetables_fchg_raw`, `psa.timetables_rchg_raw`

**Data Warehouse**:
- `dwh.stations`: Dimensionstabelle für Bahnhöfe
- `dwh.weather_forecast_vertical_raw`: Vertikalisierte Vorhersagedaten (JSONB)
- `dwh.weather_history_vertical_raw`: Vertikalisierte historische Daten (JSONB)

**Views**:
- `dwh.v_stations`: Angereicherte Stationsdimension
- `dwh.v_weather_forecast_hourly`: Stündliche Vorhersagen (duplikatfrei)
- `dwh.v_weather_history_hourly`: Stündliche historische Daten (duplikatfrei)

---

## DAGs (Airflow)

### `db_stations_import`
**Datei**: `airflow/dags/db_stations_dag.py`

**Aufgabe**: Alle Bahnhöfe via Deutsche Bahn Station Data v2 API laden

**Ablauf**:
1. Paginierte Schleife: `offset += limit` bis `total` erreicht
2. Speicherung in `stg.db_stations_raw`
3. Kopie nach `psa.db_stations_raw`
4. Bereinigung von `stg`

**Logging**:
- `metadata.api_call_log`: Quelle, Endpoint, Parameter, HTTP‑Status, Antwortzeit, Ergebnisanzahl
- `metadata.process_log`: Prozess‑Start, Ende, Status, Batch‑ID

**Fehlertoleranz**: Airflow‑Retries bei `4xx/5xx` HTTP‑Fehlern

### `db_timetables_plan_import`
**Datei**: `airflow/dags/db_timetables_plan_dag.py`

**Aufgabe**: Abruf der Fahrplan‑Daten (PLAN, XML) für Bahnhöfe im Land Bremen.

**Ablauf**:
- Auswahl der EVA‑Nummern aus `dwh.v_stations` mit Bremen‑Filter: `name LIKE 'Brem%'` und `REPLACE(ds100, '"', '') LIKE 'HB%'` (konfigurierbar über ENV, siehe unten).
- Berechnung von Datum und Stunde aus der Airflow‑`logical_date` (`YYMMDD`/`HH`).
- Pro EVA ein API‑Call an das DB Timetables‑Endpoint `plan`; Logging des Calls in `metadata.api_call_log`.
- Speicherung des rohen XML als `TEXT` direkt in `stg.timetables_plan_raw.payload` (exakt wie von der API geliefert, ohne JSON‑Umverpackung).
- Kopie nach `psa.timetables_plan_raw` und anschließendes Purge von `stg`.

**Scheduling**: `0 1 * * *` (täglich um 01:00 Uhr, Europe/Berlin).

**Begründung**: Der DAG lädt planmäßige Stundenfenster gesammelt: alle Stunden (`00–23`) des aktuellen Tages und die frühen Stunden (`00–05`) des Folgetages. Dies reduziert API‑Last und garantiert gleichzeitig vollständige Tagesabdeckung.

#### Documentation détaillée: Endpoint DB Timetables `/plan` (FR)
- Objet: fournit l’instantané des horaires PLAN (départs/arrivées) prévus pour une gare (EVA) à une heure précise.
- Base URL: `https://api.deutschebahn.com/timetable/plan/{evaNo}/{date}/{hour}`.
- Méthode: `GET`.
- Authentification: via identifiants DB API Marketplace (variables d’environnement `DB_CLIENT_ID`, `DB_API_KEY`). Les identifiants sont transmis aux requêtes selon la configuration du client HTTP.

**Paramètres**
- `evaNo`: identifiant EVA de la gare (6–7 chiffres) pour une gare du Land de Brême.
- `date`: date au format `YYMMDD` en fuseau `Europe/Berlin` (ex.: `250311` pour 11 mars 2025).
- `hour`: heure au format `HH` (`00`–`23`).

**Réponse (XML)**
- Racine: `<timetable station="...">`.
- Éléments enfant: plusieurs `<s id="...">` (un service/arrêt), contenant:
  - `<tl ...>`: métadonnées du train (catégorie `c`, numéro `n`, opérateur `o`, origine `f`, destination `t`).
  - `<dp ...>`: départ prévu avec attributs typiques `pt` (planned time, `YYMMDDHHMM`), `pp` (quai), `l` (ligne), `ppth` (chemin).
  - `<ar ...>`: arrivée prévue avec attributs analogues (`pt`, `pp`, `l`).

Exemple abrégé:

```xml
<timetable station="Bremen Hbf" eva="8000013">
  <s id="1001">
    <tl f="Oldenburg(Oldb)Hbf" t="Bremen Hbf" o="DB" c="RE" n="1"/>
    <ar pt="2503110900" pp="5" l="S"/>
  </s>
  <s id="1002">
    <tl f="Bremen Hbf" t="Vechta" o="DB" c="RB" n="2"/>
    <dp pt="2503110915" pp="7" l="S" ppth="Bremen Hbf|Delmenhorst|..."/>
  </s>
</timetable>
```

Notes:
- Le flux PLAN porte les horaires « prévus » (`pt`). Les attributs d’« heure réelle » (`rt`) sont fournis plutôt par les endpoints de changements (RCHG/FCHG).
- Les balises optionnelles comme `wings`, `ppth` peuvent apparaître selon la desserte.

**Codes de statut & erreurs**
- `200`: succès, XML retourné.
- `401`: identifiants invalides ou manquants.
- `404`: combinaison `{evaNo,date,hour}` sans données.
- `429`: dépassement de quota/rate limit.
- `5xx`: indisponibilité côté serveur.

**Orchestration Airflow (DAG `db_timetables_plan_import`)**
- Fréquence: `0 1 * * *` (tous les jours à 01:00, Europe/Berlin).
- Fenêtre ingérée à chaque run: heures `00–23` du jour Airflow (`logical_date`) + heures `00–05` du lendemain.
- Filtre gares: uniquement Brême via `dwh.v_stations` (`name LIKE 'Brem%'`, `REPLACE(ds100, '"', '') LIKE 'HB%'`).
- Stockage: insertion brute du XML en `stg.timetables_plan_raw(payload TEXT)` puis copie en `psa.timetables_plan_raw`, purge de `stg`.
- Traçabilité: chaque appel est journalisé en `metadata.api_call_log` (source, endpoint, paramètres, statut, latence, volume).

**Configuration requise**
- `DB_CLIENT_ID`, `DB_API_KEY`: identifiants DB API Marketplace.
- Filtres Brême: paramétrables au niveau des utilitaires stations (voir section Stations/Mapping ci‑dessus).
- Timezone: conversion assurée via `Europe/Berlin` dans les utilitaires de date.

**Exemples de requêtes**
- `curl` (remplacez les identifiants):

```
curl -H "Authorization: Bearer <API_KEY>" \
  "https://api.deutschebahn.com/timetable/plan/8000013/250311/09"
```

**Validation rapide (SQL)**
- Compter les entrées PLAN du jour Airflow (STG ou PSA):

```
SELECT COUNT(*)
FROM psa.timetables_plan_raw
WHERE insert_ts::date = CURRENT_DATE;
```

- Inspecter un payload et vérifier la gare:

```
SELECT SUBSTRING(payload FROM 1 FOR 200)
FROM psa.timetables_plan_raw
ORDER BY insert_ts DESC
LIMIT 1;
```

**Points d’attention**
- Fuseau horaire: toujours raisonner en `Europe/Berlin` pour `date/hour`.
- Fenêtres: il peut y avoir des services recouvrant des heures adjacentes; c’est le comportement attendu.
- Rate limits: privilégiez l’orchestration par lots (comme le DAG) pour limiter la charge.


### `db_timetables_fchg_import`
**Datei**: `airflow/dags/db_timetables_fchg_dag.py`

**Aufgabe**: Abruf der vollständigen Änderungen (FCHG, XML) für Bahnhöfe in Bremen.

**Ablauf**:
- Auswahl der EVA‑Nummern über `dwh.v_stations` mit Bremen‑Filter (wie oben).
- Pro EVA ein API‑Call an `fchg`; Logging in `metadata.api_call_log`.
- Speicherung des rohen XML als `TEXT` in `stg.timetables_fchg_raw.payload`; Kopie nach `psa.timetables_fchg_raw`; Purge von `stg`.

**Scheduling**: `*/10 * * * *` (alle 10 Minuten).

**Begründung**: Der FCHG‑Feed liefert umfangreiche Änderungsinformationen. 10‑Minuten‑Intervalle bieten zeitnahe Aktualität bei kontrollierter API‑Last.

### `db_timetables_rchg_import`
**Datei**: `airflow/dags/db_timetables_rchg_dag.py`

**Aufgabe**: Abruf der jüngsten Änderungen (RCHG, XML) für Bahnhöfe in Bremen.

**Ablauf**:
- Auswahl der EVA‑Nummern über `dwh.v_stations` mit Bremen‑Filter (wie oben).
- Pro EVA ein API‑Call an `rchg`; Logging in `metadata.api_call_log`.
- Speicherung des rohen XML als `TEXT` in `stg.timetables_rchg_raw.payload`; Kopie nach `psa.timetables_rchg_raw`; Purge von `stg`.

**Scheduling**: `*/10 * * * *` (alle 10 Minuten).

**Begründung**: RCHG liefert kompaktes Delta der jüngsten Änderungen. Ein 10‑Minuten‑Takt stellt hohe Datenfrische für betriebsnahe Analysen sicher.

#### Hilfs‑Utilities und Refactoring
- Gemeinsame Logik wurde in `airflow/include/utils` zentralisiert, um DRY und Wartbarkeit zu gewährleisten:
  - `date_utils.py`: Timezone‑Konvertierung (`Europe/Berlin`), Formate `YYMMDD`/`HH`.
  - `stations.py`: EVA‑Auswahl mit konfigurierbaren Bremen‑Filtern.
  - `timetables.py`: Orchestriert Batch‑ID‑Vergabe, API‑Aufrufe, Logging, Inserts nach `stg`, Kopie nach `psa`, Purge.
- Die drei Timetables‑DAGs sind bewusst minimalistisch und delegieren die Geschäftslogik an diese Utilities.

### `open_meteo_forecast_import`
**Datei**: `airflow/dags/open_meteo_dag.py`

**Aufgabe**: Wettervorhersagen für Stationskoordinaten in Bremen ingestieren

**Stundenparameter**: `temperature_2m`, `relative_humidity_2m`, `dew_point_2m`, `apparent_temperature`, `precipitation_probability`, `precipitation`, `rain`, `showers`, `snowfall`, `snow_depth`, `weather_code`, `pressure_msl`, `surface_pressure`, `cloud_cover`, `cloud_cover_low`, `cloud_cover_mid`, `cloud_cover_high`, `visibility`, `wind_speed_10m`, `evapotranspiration`, `vapour_pressure_deficit`

**Batching**: Koordinaten in Pakete von maximal 200 Stationen zur URL‑Längen‑ und Lastbegrenzung

**Tasks**:
1. `fetch_open_meteo_weather`: API‑Call und Speicherung in STG
2. `copy_weather_to_psa`: Kopie nach PSA
3. `purge_stg_weather`: Bereinigung STG
4. `transform_forecast_to_dwh`: Vertikalisierung und Speicherung in DWH mit Idempotenzprüfung

### Änderungen und Updates (Timetables & DWH)
Zur besseren analytischen Nutzung der Fahrplandaten wurden folgende Änderungen implementiert:
- FCHG‑Transformation (DWH): Die Ereignistabelle nutzt jetzt `eva_number` als Stationskennung und ist vollständig dokumentiert (Spaltenkommentare). Pro Meldung wird eine Zeile mit Zeit, Typ, Kategorie, Priorität, Verspätung und Bahnsteigwechsel erzeugt.
- PLAN‑Transformation (DWH): Neuer Speicherpfad für planmäßige Abfahrts‑/Ankunftsereignisse in `dwh.timetables_plan_events` mit den Feldern Stationsname, Service‑ID, Zugnummer, Zuggattung, Typ, Richtung, Ereignistyp (`dp/ar`), Zeit, Gleis, `train_line_name`, Route und Batch. Der DAG `db_timetables_plan_import` führt die Transformation automatisch nach der Ingestion aus.
 - RCHG‑Transformation (DWH): Jüngste Änderungen (`m/ar/dp`) werden in `dwh.timetables_rchg_events` gespeichert. Eine Zeile je Ereignis mit klaren Feldern (Ereignis‑ID, EVA, Station, Nachricht/Typ/Kategorie/Änderungstyp/Priorität, Gültigkeiten, alte/neue Zeit, Verspätung, Gleis/Wechsel, Route, `train_line_name`, Ereigniszeit, Batch) und garantierter Deduplikation über `event_hash`. Der DAG `db_timetables_rchg_import` führt die Transformation direkt nach der Ingestion aus.
 - RCHG‑Schema‑Anpassung: Entfernung der Felder `train_number`, `train_category`, `train_type`, `train_direction`; Feld `train_line_name` bleibt erhalten (Anforderung Fachlogik).
- Idempotenz und Logging: Beide Transformationen protokollieren `success/skipped` in `metadata.process_log` und verhindern doppelte Einfügungen.
- Indizes: Selektive Indizes auf Station und Zeit wurden ergänzt, um typische Abfragen (Fenster, Bahnhof) zu beschleunigen.

**Besonderheit**: Transformation ist integriert; bereits verarbeitete Batches werden als `skipped` protokolliert; für RCHG verhindert ein eindeutiger Ereignis‑Hash (`event_hash`) Duplikate.

### `open_meteo_archive_import`
**Datei**: `airflow/dags/open_meteo_archive_dag.py`

**Aufgabe**: Historische Open‑Meteo‑Daten über konfigurierbares Zeitfenster laden

**Parameter**: `ARCHIVE_START_DATE`, `OPEN_METEO_ARCHIVE_HOURLY` (Ende automatisch J‑2)

**Tasks**:
1. `fetch_and_store_weather_archive`: API‑Call und Speicherung in STG
2. `copy_weather_archive_to_psa`: Kopie nach PSA
3. `purge_stg_weather_archive`: Bereinigung STG
4. `transform_history_to_dwh`: Vertikalisierung und Speicherung in DWH mit Idempotenzprüfung

**Regionale Einschränkung**: Es werden ausschließlich Koordinaten für Bremen über `get_bremen_station_coords()` verwendet; die API wird nur für diese Koordinaten aufgerufen.

**Besonderheit**: Fenster rollierend
- Erstlauf: `start_date = ARCHIVE_START_DATE` (z. B. `2025‑01‑01`), `end_date = heute‑2 Tage`
- Folgeläufe: `start_date = letztes end_date` (aus `metadata.api_call_log`), `end_date = heute‑2 Tage`
- Inklusive Fenster (Start = letztes End): Duplikate werden durch Views (`DISTINCT ON`) eliminiert; optional kann auf exklusiv (Start = letztes End + 1 Tag) umgestellt werden

---

## Python‑Utilities

### `utils/api_client.py`
**Funktionen**:
- `fetch_db_stations_all`: Paginierte Schleife mit Zeitmessung; Rückgabe `(items, status, elapsed, meta)`
- `fetch_open_meteo_forecast`: URL‑Bau mit gerundeten Koordinatenlisten; Timeout und JSON‑Parsing
- `fetch_open_meteo_archive`: Analog zu Forecast mit `start_date`, `end_date` und `hourly`‑Parametern

### Refactorisation des utilitaires DB
Le module `utils/db.py` sert désormais de façade légère et ré‑exporte des fonctions depuis des modules spécialisés. Ceci améliore la lisibilité, supprime les doublons et maintient la compatibilité des imports existants.

**Modules introduits**:
- `utils/db_conn.py`: paramètres de connexion DB via ENV; `get_connection()` centralisé
- `utils/db_insert.py`: `insert_json`, `insert_text` (insertion bulk, support `batch_id`)
- `utils/db_logs.py`: `log_api_call`, `log_process_start`, `log_process_end`, `is_batch_processed`
- `utils/db_batch.py`: `_get_latest_batch_id`, `get_latest_forecast_batch_id`, `get_latest_history_batch_id`, `get_last_archive_end_date`
- `utils/db_stg_psa.py`: `copy_*` et `purge_*` pour Stations, Forecast, Archive, Timetables (STG → PSA)
- `utils/weather_transform.py`: `insert_forecast_verticalized_to_dwh`, `insert_history_verticalized_to_dwh`, mapping station (tolérance 1e‑4)

**Façade `utils/db.py`**:
- Expose toutes les fonctions ci‑dessus pour compatibilité (`from utils import db as db_utils`)
- Conserve les transformations Timetables vers DWH: `insert_timetables_fchg_events_to_dwh`, `insert_timetables_plan_events_to_dwh`, `insert_timetables_rchg_events_to_dwh`
- Continue d’utiliser les inserts en masse (`psycopg2.extras.execute_values`) et l’idempotence via `metadata.process_log`

---

## Logik der Vertikalisierung (DWH)

### Prinzipien
- **Ausrichtungsachse**: `hourly.time` dient als Zeilenindex
- **Extraktion**: Am Index `i` wird für jede Messung `hourly.<measure>[i]` extrahiert; fehlende Werte → `NULL`
- **Metadaten**: Jede Zeile enthält `psa_id`, `station_id`, `batch_id`, `latitude`, `longitude`

### Forecast (`insert_forecast_verticalized_to_dwh`)
**Messungen**: Temperatur, Luftfeuchte, Taupunkt, gefühlte Temperatur, Niederschlagswahrscheinlichkeit und ‑summe, Regen, Schauer, Schnee, Schneehöhe, Wettercode, Druck (Meeresspiegel und Oberfläche), Wolkenbedeckung (gesamt, niedrig, mittel, hoch), Sichtweite, Windgeschwindigkeit, Evapotranspiration, Dampfdruckdefizit

**Robustheit**: Toleranz gegenüber teilweisen Payloads; fehlende Felder werden als `NULL` gespeichert

### History (`insert_history_verticalized_to_dwh`)
**Eingabe**: Liste, Einzelobjekt oder Dictionary mit Schlüssel `results` (automatisch erkannt)

**Messungen**: Analog zu Forecast, ohne Niederschlagswahrscheinlichkeit, ohne Evapotranspiration und Dampfdruckdefizit

### Views `dwh.v_weather_*_hourly`
**Funktionen**:
- Extraktion und Typkonvertierung von JSONB‑Feldern
- Join mit `dwh.v_stations` für Stationsinformationen (Name, DS100, EVA‑Nummer)
- **Automatische Duplikateliminierung**: `DISTINCT ON (station_id, time)` mit `ORDER BY batch_id DESC`

**Garantie**: Konsumenten erhalten immer nur die aktuellste Messung pro Station und Zeitpunkt

---

## Nützliche Befehle

### DAG‑Operationen
- Forecast auslösen: `docker exec airflow_webserver bash -lc "airflow dags trigger open_meteo_forecast_import"`
- Archive auslösen: `docker exec airflow_webserver bash -lc "airflow dags trigger open_meteo_archive_import"`
- Task testen: `docker exec airflow_scheduler bash -lc "airflow tasks test db_stations_import fetch_and_store_stations YYYY-MM-DD"`

### Datenzählungen
- `SELECT COUNT(*) FROM psa.weather_forecast_raw;`
- `SELECT COUNT(*) FROM dwh.weather_forecast_vertical_raw;`
- `SELECT COUNT(*) FROM dwh.v_weather_forecast_hourly;`

### Duplikate prüfen
**Forecast**:
```sql
SELECT station_id, time, COUNT(*) as nb_doublons
FROM dwh.v_weather_forecast_hourly
GROUP BY station_id, time
HAVING COUNT(*) > 1;
```

**History**:
```sql
SELECT station_id, time, COUNT(*) as nb_doublons
FROM dwh.v_weather_history_hourly
GROUP BY station_id, time
HAVING COUNT(*) > 1;
```

**Erwartetes Ergebnis**: 0 Zeilen (Views eliminieren automatisch Duplikate)

### Datenabfragen
- Vorhersagen: `SELECT time, temperature_2m, precipitation FROM dwh.v_weather_forecast_hourly ORDER BY time ASC LIMIT 10;`
- Stationen: `SELECT station_id, name, latitude, longitude FROM dwh.v_stations ORDER BY created_at DESC LIMIT 10;`

---

## Troubleshooting

### Airflow 403‑Fehler
**Problem**: Webserver zeigt 403‑Fehler in Logs

**Lösung**: `AIRFLOW__WEBSERVER__SECRET_KEY` für Webserver und Scheduler identisch setzen

### API 401/403‑Fehler
**Problem**: Deutsche Bahn API antwortet mit Authentifizierungsfehler

**Lösung**: `DB_CLIENT_ID` und `DB_API_KEY` prüfen; API‑Quotas überprüfen

### Datenbank nicht bereit
**Problem**: Airflow kann keine Verbindung zur Datenbank herstellen

**Lösung**: Healthcheck des `postgres`‑Dienstes abwarten; `db-init` ist idempotent

### Duplikate in Views
**Problem**: Views zeigen doppelte Einträge für `(station_id, time)`

**Diagnose**: View‑Definition prüfen; `DISTINCT ON` muss korrekt implementiert sein

**Hinweis**: Bei korrekter Implementation sollten niemals Duplikate auftreten

---

## Grenzen und Verbesserungen

### DWH‑Duplikateliminierung
**Aktueller Stand**: View‑basierte Lösung mit `DISTINCT ON`

**Vorteile**:
- Maximale Flexibilität; alle historischen Batches bleiben verfügbar
- Einfache Wartung und Versionierung

**Nachteile**:
- Potenzielle Performance‑Einbußen bei sehr großen Datenmengen

**Verbesserungsoptionen**:
- Composite Index auf `(payload->>'station_id', payload->>'time', batch_id DESC)`
- Materialisierte Views bei Performance‑Problemen
- Partitionierung der Rohdatentabellen nach Zeitfenster

**Alternative Ansätze** (für zukünftige Iterationen):
- `ON CONFLICT DO NOTHING` in Rohdatentabellen (verliert historische Batches)
- Periodische Cleanup‑Jobs (zusätzliche Komplexität)

### Performance
**Empfohlene Optimierungen**:
- Index auf `payload->>'time'` (ISO‑Zeitstempel sind lexikographisch sortierbar)
- Index auf `payload->>'station_id'`
- Materialisierte Views für `v_weather_*_hourly` bei hoher Volumetrie
- Regelmäßige `VACUUM ANALYZE` auf JSONB‑Tabellen

### Datenqualität
**Erweiterungsmöglichkeiten**:
- Python‑Validierung: Konsistenz aller `hourly.*`‑Arraylängen prüfen
- Anomalieerkennung: Unrealistische Messwerte loggen
- Data Quality Checks als eigene Airflow‑Tasks

### Funktionserweiterungen
**Zukünftige Features**:
- Weitere Open‑Meteo‑Messungen (Windrichtung, Böen, UV‑Index)
- ML‑DAG zur Vorhersage von Bahnbeeinträchtigungen aus Zeitreihen
- Echtzeit‑Alerts bei extremen Wetterbedingungen
- Grafana‑Dashboards für Monitoring

---

## Glossar

- **STG**: Staging (Rohdaten, flüchtig)
- **PSA**: Persistent Staging Area (unveränderliche Kopie für Audit und Reprocessing)
- **DWH**: Data Warehouse (Modellierung und Konsum)
- **Vertikalisierung**: Umwandlung von parallelen Arrays in zeilenorientiertes Format
- **batch_id**: Eindeutige Laufkennung für vollständige Nachverfolgbarkeit
- **DISTINCT ON**: PostgreSQL‑Feature zur Duplikateliminierung basierend auf bestimmten Spalten
- **Idempotenz**: Eigenschaft, dass wiederholte Ausführungen dasselbe Ergebnis liefern
- **ELT**: Extract, Load, Transform (Transformation nach der Speicherung)

---

## Architekturziele und Begründungen

### Entkopplung durch Schichten
**Ziel**: Saubere Trennung von Aufnahme, Audit und Modellierung

**Effekte**:
- Fehler lassen sich isoliert betrachten
- Reprocessing benötigt nur PSA; Ingestion bleibt unberührt
- Klare Verantwortlichkeiten pro Schicht
- Unabhängige Evolution von Ingestion und Transformation

### ELT mit JSONB
**Entscheidung**: Rohdaten als JSONB speichern; Extraktion über deklarative Views

**Vorteile**:
- Flexibilität bei Schemaänderungen
- Geringe Aufnahmezeit
- Klare, versionierbare Extraktion
- Volle API‑Response für Audits verfügbar

**Trade‑off**: Etwas langsamere Abfragen als bei vollständiger Normalisierung

**Gegenmaßnahmen**: Indizes, materialisierte Views

### Duplikateliminierung auf View‑Ebene
**Entscheidung**: `DISTINCT ON` in Konsum‑Views statt Constraints in Rohdaten

**Vorteile**:
- Alle historischen Daten bleiben für Audits verfügbar
- Einfache Wartung (deklarativ in SQL)
- Konsumenten sehen automatisch nur aktuelle Daten
- Keine komplexe Cleanup‑Logik erforderlich

**Trade‑off**: Performance bei sehr großen Datenmengen

**Gegenmaßnahmen**: Indizes, materialisierte Views, Partitionierung

### Transformation innerhalb der Import‑DAGs
**Entscheidung**: Kein separater Transform‑DAG

**Vorteile**:
- Geringere Latenz
- Einfachere Koordination
- Klarer, linearer Prozessfluss
- Atomarität: Ein Lauf = vollständiger Durchlauf

**Trade‑off**: Geringere Entkopplung

**Gegenmaßnahmen**: Idempotenz, umfassendes Logging

### Idempotenz und Duplikatvermeidung
**Mechanismus**: Prüfung vor Transformation; bereits verarbeitete Batches werden übersprungen

**Vorteile**:
- Airflow‑Retries sind sicher
- Manuelle Re‑Runs führen nicht zu Datenduplikaten
- Audits bleiben konsistent

**Implementierung**: `is_batch_processed` in `utils.db`

### Batching und API‑Laststeuerung
**Forecast**: Koordinaten‑Batches (max. 200)

**Vorteile**:
- Respektiert URL‑Längenbeschränkungen
- Lastverteilung auf API
- Bessere Fehler‑Isolation

**Archive**: Fenster rollierend
- Erstlauf: `start_date = ARCHIVE_START_DATE` (z. B. `2025‑01‑01`), `end_date = heute‑2 Tage`
- Folgeläufe: `start_date = letztes end_date` (aus `metadata.api_call_log`), `end_date = heute‑2 Tage`

**Vorteile**:
- Kontrollierte historische Verarbeitung
- Flexibilität für Backfills

### Scheduling und Catchup
**Forecast**: `0 */6 * * *`, `catchup=false`
- Stündlich im 6‑Stunden‑Raster (00/06/12/18) für laufende Vorhersageaktualisierung

**Archive**: `0 3 * * *`, `catchup=false`
- Standardfenster J‑2 zur Sicherung stabiler Daten
 - Hinweis: Fenster rollierend (siehe Abschnitt „Batching und API‑Laststeuerung“)
- Initial Load: explizites Startdatum über `ARCHIVE_START_DATE`; Enddatum ist immer „heute‑2 Tage“ (J‑2) oder temporär `catchup=true`

---

## Fehlerbehandlung, Observability und Betrieb

### Logging und Nachverfolgbarkeit
**Komponenten**:
- `metadata.api_call_log`: Vollständige Parameter/Antwortmetrik pro API‑Abruf
- `metadata.process_log`: Start/Ende, Status, Nachrichten, Batch‑ID

**Nutzen**:
- Vollständiger Audit‑Trail
- Performance‑Monitoring
- Debugging bei Fehlern
- Compliance‑Anforderungen

### Retries und Robustheit
**Airflow‑Retries**: Automatisch bei transienten Fehlern

**Idempotenz**: Schützt## Timetables FCHG  DWH-Transformation
- DWH-Tabelle: dwh.timetables_fchg_events (EVA-basiert).
- Felder: eva_number, station_name, event_time (ct bevorzugt, sonst ts), message_id, 	ype (d/f/h), category, priority, delay_minutes, alid_from, alid_to, platform_change, atch_id.
- Im DAG db_timetables_fchg_import wird 	ransform_fchg_to_dwh am Ende ausgef�hrt.
# Table DWH `timetables_fchg_events`

Cette table contient une version normalisée des événements issus du flux FCHG de la Deutsche Bahn pour chaque gare (EVA). Elle est alimentée à la fin du DAG `dimport_db_timetables_fchg_import` par une transformation qui parse les XML bruts stockés dans PSA.

## Attributs et sémantique

- **eva_number** : identifiant EVA unique de la gare (source : attribut `eva` de la racine `<timetable>`).  
- **station_name** : nom de la gare (source : attribut `station` de la racine `<timetable>`).  
- **event_time** : horodatage de l’événement. Priorité au champ `ct` du parent `<ar>/<dp>` lorsqu’il existe (heure calculée côté DB), sinon `ts` du message `<m>`. Stocké avec timezone pour une interprétation fiable.  
- **message_id** : identifiant unique du message `<m>`. Permet la traçabilité fine et le regroupement.  
- **type** : type d’événement tel que fourni par l’API (`d` pour départ, `f` pour arrivée, `h` pour information). Les autres types éventuels sont conservés tels quels.  
- **category** : catégorie métier de la notification lorsque disponible au niveau `<s>` (ex. `Störung` pour incident, `Information`). Les messages sous `<ar>/<dp>` n’incluent pas de catégorie (champ vide).  
- **priority** : priorité (`pr`) de la notification, si fournie par l’API.  
- **delay_minutes** : code de retard (`c`) en minutes issu des messages `<ar>` et `<dp>`. Vide pour les messages de type `h`.  
- **valid_from** : début de validité (`from`) d’une information ou d’un incident (messages de type `h`). Stocké avec timezone.  
- **valid_to** : fin de validité (`to`) d’une information ou d’un incident (messages de type `h`). Stocké avec timezone.  
- **platform_change** : indicateur de changement de quai/voie (`cp`) présent sur `<ar>/<dp>` lorsqu’applicable.  
- **batch_id** : identifiant de lot du run ETL ; sert à l’idempotence et à l’audit (lié à `metadata.process_log`).  
- **inserted_at** : timestamp d’insertion en DWH.

## Règles de peuplement et transformation

- Une ligne est créée par message `<m>` rencontré dans le XML.  
- Les timestamps bruts (format `YYMMDDHHMM`) sont convertis en datetimes standards.  
- La transformation ne modifie pas les payloads PSA ; elle lit depuis `psa.timetables_fchg_raw` et écrit uniquement dans `dwh.timetables_fchg_events`.  
- La transformation est idempotente : un batch déjà transformé est marqué `skipped` dans `metadata.process_log` et n’est pas réinséré.

## Index et performance

- Index sur **eva_number** pour les requêtes par gare.  
- Index sur **event_time** pour les fenêtres temporelles.  
- Index sur **category** pour filtrer rapidement les incidents.

## Utilisation analytique

- **Analyse des incidents ferroviaires** : filtrer `type = 'h'` et `category = 'Störung'`.  
- **Suivi des retards** : filtrer `type IN ('d','f')` et `delay_minutes IS NOT NULL`.  
- **Agrégations par gare et par heure** : compter les événements, calculer des indicateurs (retard moyen, volume d’incidents), préparer des features pour le machine learning.
