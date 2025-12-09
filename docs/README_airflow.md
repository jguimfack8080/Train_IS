# Airflow – Orchestrierung

## Ziele
- Steuerung der ETL-Prozesse: Abruf, Transformation, Speicherung, Logging, (später) Modelltraining.
- Jeder Prozess ist als DAG definiert, mit klaren Aufgaben und Abhängigkeiten.

## Ausführung
- Weboberfläche: `http://localhost:8085`
- Standard-Login: `admin` / `admin`
- Executor: `LocalExecutor`
- Metadatenbank: Postgres-Datenbank `airflow`

## DAGs
- `db_stations_import`
  - Ruft alle verfügbaren Stationen der Deutschen Bahn API ab (`limit=100`, `searchstring=*`).
  - Speichert Rohdaten in `stg.db_stations_raw` (JSONB) – Eintrag pro Station (Batch‑Insert).
  - Loggt den API-Call in `metadata.api_call_log`.

- `open_meteo_forecast_import`
  - Liest Koordinaten aus `dwh.v_stations` mit Filter Bremen.
  - Batches bis zu 200 Koordinaten pro Anfrage.
  - Speichert Vorhersage‑Rohdaten in `stg.weather_forecast_raw` (JSONB) und loggt den API‑Call; Kopie nach `psa.weather_forecast_raw`, Purge `stg`.
  - Tasks: `fetch_open_meteo_weather` → `copy_weather_to_psa` → `purge_stg_weather` → `transform_forecast_to_dwh`.
  - Transformation: innerhalb dieses DAGs mit Idempotenzprüfung (`utils.db.is_batch_processed`); bereits verarbeitete Batches werden als `skipped` protokolliert.

- `open_meteo_archive_import`
  - Historische Wetterdaten (Archive) für Stationskoordinaten,
  - Speichert Rohdaten in `stg.weather_history_raw` (JSONB) und loggt den API-Call; Kopie nach `psa.weather_history_raw`, Purge `stg`.
  - Über Env konfigurierbar: `ARCHIVE_START_DATE`, `OPEN_METEO_ARCHIVE_HOURLY`; Enddatum wird automatisch als J‑2 (heute minus zwei Tage) berechnet.
  - Tasks: `fetch_and_store_weather_archive` → `copy_weather_archive_to_psa` → `purge_stg_weather_archive` → `transform_history_to_dwh`.
  - Transformation: innerhalb dieses DAGs mit Idempotenzprüfung; bereits verarbeitete Batches werden als `skipped` protokolliert.

- Nachverfolgbarkeit via `psa_id` und `batch_id`; Views: `dwh.v_weather_forecast_hourly`, `dwh.v_weather_history_hourly`.

### Timetables (PLAN/FCHG/RCHG)
- Import‑DAGs: `db_timetables_plan_import`, `db_timetables_fchg_import`, `db_timetables_rchg_import`
- Speicherung: Roh‑XML in `stg.timetables_*_raw(payload TEXT)`, Kopie nach `psa.timetables_*_raw`
- Transformation (FCHG): Aufgabe `transform_fchg_to_dwh` schreibt Ereignisse nach `dwh.timetables_fchg_events` mit Feldern
  - `eva_number`, `station_name`, `event_time` (bevorzugt `ct` aus `<ar>/<dp>`, sonst `ts` aus `<m>`)
  - `message_id`, `type` (`d`/`f`/`h`), `category` (z. B. `Störung`, `Information`), `priority`
  - `delay_minutes` (`c`), `valid_from`/`valid_to` (`from`/`to`), `platform_change` (`cp`)
  - `batch_id`

## Umgebungsvariablen
- `DB_CLIENT_ID`, `DB_API_KEY` – Deutsche Bahn API Credentials.
- `DATA_DB_*` – Verbindungsdaten zur DWH-Datenbank (`train_dw`).
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` – Connection-String zur Airflow-Metadatenbank.

## Entwicklung
- DAG-Code liegt unter `airflow/dags`.
- Hilfsfunktionen liegen unter `airflow/include/utils`.
- Python-Abhängigkeiten: `airflow/requirements.txt`.

## Hinweise
- Bei ungültigen DB-API-Credentials liefert die API Fehler (z. B. 401/403).  
- Transformationen (STG→PSA→DWH) laufen innerhalb der Open‑Meteo‑DAGs.

## Schnelle Tests (CLI)
- Einzeltask testen: `docker exec airflow_scheduler bash -lc "airflow tasks test db_stations_import fetch_and_store_stations YYYY-MM-DD"`
- Zeilen zählen: `docker exec train_postgres psql -U dw -d train_dw -c "SELECT COUNT(*) FROM stg.db_stations_raw;"`
- Beispielstationen: `docker exec train_postgres psql -U dw -d train_dw -c "SELECT payload->>'name' FROM stg.db_stations_raw ORDER BY id ASC LIMIT 10;"`

## Architekturprinzipien und Begründungen
- Schichtenmodell (`stg` → `psa` → `dwh`)
  - Begründung: strikte Trennung von Aufnahme (flüchtig), auditierbarer Kopie (unverändert) und analytischer Modellierung erhöht Nachvollziehbarkeit, Wiederaufsetzbarkeit und Performance.
  - Vorteile: reproduzierbare Läufe, einfache Fehleranalyse, geringere Kopplung zwischen Ingestion und Konsum.
- Transformation im Import‑DAG
  - Entscheidung: Die DWH‑Transformation läuft direkt am Ende jedes Import‑DAGs (Forecast/Archive) statt in einem separaten Transform‑DAG.
  - Begründung: geringere Latenz, weniger Koordinationskomplexität (keine ExternalTaskSensoren/Trigger nötig), klarer, linearer Fluss.
  - Trade‑off: geringere Entkopplung; mitigiert durch Idempotenz (siehe unten) und sauberes Logging.
- Idempotenz auf Prozessebene
  - Umsetzung: vor jedem DWH‑Insert prüft `utils.db.is_batch_processed(process_name, batch_id)`, ob der Batch bereits erfolgreich verarbeitet wurde.
  - Begründung: verhindert Duplikate, erlaubt jederzeitige Wiederholung von Importen ohne Datenfehler; Status `skipped` wird protokolliert.
- Batch‑Verarbeitung und API‑Laststeuerung
  - Forecast: Koordinaten werden in Batches (max. ~200) angefragt, um URL‑Länge und Rate‑Limits stabil einzuhalten.
  - Archive: Startfenster via ENV (`ARCHIVE_START_DATE`) und Granularität (`OPEN_METEO_ARCHIVE_HOURLY`); Enddatum ist immer „heute‑2 Tage“ (J‑2) und wird automatisch gesetzt.
- ELT‑Ansatz mit JSONB
  - Entscheidung: Rohpayloads in JSONB speichern und über Views konsumieren.
  - Hinweis: Timetables‑Feeds liefern XML und werden unverändert als `TEXT` gespeichert (kein JSON‑Wrapping), um den API‑Return exakt zu erhalten.
  - Begründung: flexible Schemaentwicklung, schnelle Aufnahme, geringere Migrationskosten; klare, versionierbare Extraktion in `v_weather_*_hourly`.
– Scheduling und Catchup
  - Forecast: `0 */6 * * *`, `catchup=false` (laufende Aktualisierung der Prognosen über den Tag).
  - Archive: `0 3 * * *`, `catchup=false` (Standardfenster J‑2 für stabile Daten). Initial Load: explizites Startdatum über `ARCHIVE_START_DATE`; Enddatum ist immer „heute‑2 Tage“ (J‑2). Alternativ temporär `catchup=true`.

## Fehlerbehandlung und Observability
- Logging
  - `metadata.api_call_log`: Quelle, Endpoint, Parameter, Status, Zeit, Resultanzahl.
  - `metadata.process_log`: Prozessstart/ende, Status (`success`, `error`, `skipped`), Nachrichten, `batch_id`.
- Retries und Robustheit
  - Standard‑Retries in Airflow; Netzwerk‑ und API‑Störungen werden durch erneute Läufe abgefedert.
  - Idempotenz schützt vor Doppelverarbeitung bei erneuten Ausführungen.
- Monitoring (optional)
  - Empfehlung: Metriken (z. B. Zeilenzahl je Batch) über StatsD/Prometheus emittieren; Alarmierung bei Fehlern.

## Betrieb, Sicherheit und Skalierung
- Secrets und Schlüssel
  - Airflow‑Schlüssel (`AIRFLOW_FERNET_KEY`, `AIRFLOW_WEBSERVER_SECRET_KEY`) konsistent setzen; keine Klartextzugänge im Code.
- Performance und Skalierung
  - `psycopg2.extras.execute_values` für Bulk‑Insert; Indizes/Materialized Views für häufige Abfragen.
  - Parallele DAG‑Runs vorsichtig dosieren; Datenbankkapazitäten beobachten.

## Tests und Validierung
- Airflow‑Task Tests
  - `airflow tasks test open_meteo_forecast_import transform_forecast_to_dwh YYYY-MM-DD`
  - `airflow tasks test open_meteo_archive_import transform_history_to_dwh YYYY-MM-DD`
- SQL‑Validierung
  - Zählungen in `psa.weather_*_raw` und `dwh.weather_*_vertical_raw` vergleichen.
  - Stichproben aus `dwh.v_weather_*_hourly` prüfen (Zeit, Station, Messwerte).

## Migrationshinweis
- Der frühere DAG `weather_transform_dwh` wurde entfernt.
- Die Transformationsschritte sind nun fest in `open_meteo_forecast_import` und `open_meteo_archive_import` integriert und idempotent.

## Änderungen und Updates (Timetables & DWH)
In den Timetables‑Pipelines wurden folgende Aktualisierungen vorgenommen, um Analyse und ML zu erleichtern:
- FCHG‑Transformation: Die DWH‑Tabelle verwendet jetzt das Stationsfeld `eva_number` anstelle von `station_id`. Die Aufgabe `transform_fchg_to_dwh` erzeugt pro Meldung eine Zeile mit Zeit, Typ, Kategorie, Priorität, Verspätung und Bahnsteigwechsel.
- PLAN‑Transformation: Der DAG `db_timetables_plan_import` wurde erweitert um die Aufgabe `transform_plan_to_dwh`. Diese schreibt planmäßige Abfahrts‑/Ankunftsereignisse (Felder: Stationsname, Service‑ID, Zugnummer, Zuggattung, Typ, Richtung, Ereignistyp `dp/ar`, Zeit, Gleis, `train_line_name`, Route, Batch) in `dwh.timetables_plan_events`.
- Idempotenz: Beide Transformationen protokollieren ihren Erfolg in `metadata.process_log` und überspringen bereits verarbeitete Batches.
- Indizes: Selektive Indizes auf Zeit und Station verbessern die Abfrageleistung in den neuen Tabellen.
