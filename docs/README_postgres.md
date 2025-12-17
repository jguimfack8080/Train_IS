# PostgreSQL – Datenarchitektur

## Schemas
- `stg` – Rohdaten (JSONB), unverändert, temporär.
- `psa` – persistente Rohdaten für Reprocessing.
- `dwh` – bereinigte, transformierte Daten.
- `metadata` – Prozessstatus, Ladehistorie, API-Logs.

## Initialisierung
- Script: `postgres/init/01_init.sql`
- Erstellt Benutzer/DBs (`airflow`, `train_dw`) und alle Schemas/Tabellen.
 - `train_dw` wird mit Owner `dw` erstellt (kein `ALTER`). Der Container‑Parameter `POSTGRES_DB` ist `postgres`, um Owner‑Konflikte beim ersten Start zu vermeiden.

## Tabellen (Auszug)
- `metadata.api_call_log` – Quelle, Endpoint, Params, Status-Code, Dauer, Anzahl.
- `metadata.process_log` – Prozessstatus und Zeitstempel.
- `stg.db_stations_raw` – Rohdaten der Deutsche Bahn Stationen.
 - Open‑Meteo: `stg.weather_forecast_raw`, `stg.weather_history_raw` (JSONB)
 - Timetables (XML, `payload TEXT`): `stg.timetables_plan_raw`, `stg.timetables_fchg_raw`, `stg.timetables_rchg_raw`
- `psa.*` – persistente Kopien der Rohdaten.
- `dwh.stations` – bereinigte Stationsdaten mit Koordinaten.
- `dwh.v_weather_forecast_hourly`, `dwh.v_weather_history_hourly` – aufbereitete Wetterzeitreihen (Views lesen aus `psa.weather_forecast_vertical_raw` und `psa.weather_history_vertical_raw`).
- `dwh.timetables_fchg_events` – transformierte Ereignisse aus FCHG (EVA‑basiert; eine Zeile je Ereignis mit `eva_number`, `station_name`, `event_time`, `type`, `category`, `priority`, `delay_minutes`, `valid_from`, `valid_to`, `platform_change`, `batch_id`).
- `dwh.timetables_plan_events` – transformierte PLAN‑Ereignisse (eine Zeile je Abfahrt/Ankunft; Felder: `station_name`, `train_line_ride_id`, `train_number`, `train_category`, `train_type`, `train_direction`, `event_type`, `event_time`, `platform`, `train_line_name`, `route_path`, `batch_id`).
 - `dwh.timetables_rchg_events` – transformierte RCHG‑Ereignisse (Recent Changes; eine Zeile je Nachricht oder Knotenereignis `m/ar/dp`; Felder: `event_id`, `eva_number`, `station_name`, `message_id`, `event_type`, `category`, `change_type`, `priority`, `valid_from`, `valid_to`, `old_time`, `new_time`, `delay_minutes`, `platform`, `platform_change`, `route`, `train_line_name`, `timestamp_event`, `batch_id`, `event_hash`).

## Änderungen und Updates
- DWH FCHG: Umstellung auf `eva_number` als Stationsschlüssel; ausführliche Spaltenkommentare hinzugefügt (Beschreibung, Herkunft, Typen).
- DWH PLAN: Neue Tabelle `dwh.timetables_plan_events` zur Speicherung planmäßiger Ereignisse; Indizes auf Station und Zeit; Spaltenkommentare in SQL ergänzt.
- DWH RCHG: Neue Tabelle `dwh.timetables_rchg_events` mit eindeutiger Deduplikation über `event_hash`, normalisierten Zeitstempeln (ISO) und klaren Attributen für Analyse/ML.
- Indizes: Ergänzung/Anpassung für schnelle Filterung nach Station, Zeit und Kategorie in Timetables‑Tabellen.

## Zugriff aus Airflow
- Verbindungsparameter via Umgebungsvariablen `DATA_DB_*`.
- Einfügungen erfolgen als JSONB (Rohpayload) in STG.
- Logs werden in `metadata.api_call_log` geschrieben.
 - Insert‑Logik: Bei Listen wird jeder Eintrag als eigene Zeile eingefügt (Batch‑Insert via `execute_values`).

## Prinzipien
- Keine Code-Duplizierung: Utility-Funktionen für Inserts/Logs.
- Saubere Trennung: STG (roh) → PSA (roh, persistent) → DWH (bereinigt).
- Reproduzierbarkeit: METADATA protokolliert jede Phase und jeden API-Call.
- Konsistente Zeitstempel: Gesamtes System (DB, Airflow, Container) läuft auf `Europe/Berlin` zur Vermeidung von UTC-Offset-Verwirrung bei lokalen Analysen.

## Troubleshooting: Zeitzonen-Problem (Gelöst)
**Symptom:**
In den Datenbanktabellen (z.B. `metadata.process_log`, `stg.*`) erschienen Zeitstempel (`ingested_at`, `started_at`) mit einem Versatz von -1 Stunde im Vergleich zur lokalen Zeit (Deutschland/Winterzeit).

**Ursache:**
- Die Spalten nutzen `DEFAULT NOW()`.
- Der PostgreSQL-Container lief standardmäßig in `UTC` (Coordinated Universal Time).
- `NOW()` lieferte daher die UTC-Zeit (z.B. 13:00 statt 14:00).
- Airflow und das Host-System liefen bereits auf `Europe/Berlin`.

**Lösung (Umgesetzt am 2025-12-17):**
1.  **Infrastructure-Level:** Environment-Variable `TZ: Europe/Berlin` zum `postgres`-Service in `docker-compose.yml` hinzugefügt.
2.  **Hotfix:** Laufenden Container via `ALTER ROLE/DATABASE ... SET timezone ...` auf Berlin umgestellt, ohne Neustart.
3.  **Daten-Integrität:** Historische Daten waren korrekt (absolute Zeitpunkte), wurden nur "falsch" (in UTC) angezeigt. Nach der Umstellung werden auch alte Einträge korrekt in lokaler Zeit dargestellt.

**Ausgeführte Hotfix-Befehle:**
```bash
# 1. Status prüfen (vorher)
docker exec train_postgres psql -U postgres -c "SHOW timezone;"
docker exec train_postgres psql -U postgres -c "SELECT NOW();"

# 2. Konfiguration anpassen (Hotfix für laufenden Container)
docker exec train_postgres psql -U postgres -c "ALTER ROLE postgres SET timezone TO 'Europe/Berlin';"
docker exec train_postgres psql -U postgres -c "ALTER ROLE airflow SET timezone TO 'Europe/Berlin';"
docker exec train_postgres psql -U postgres -c "ALTER ROLE dw SET timezone TO 'Europe/Berlin';"
docker exec train_postgres psql -U postgres -c "ALTER DATABASE airflow SET timezone TO 'Europe/Berlin';"
docker exec train_postgres psql -U postgres -c "ALTER DATABASE train_dw SET timezone TO 'Europe/Berlin';"

# 3. Verifikation
docker exec train_postgres psql -U postgres -d train_dw -c "SHOW timezone;"
docker exec train_postgres psql -U postgres -d train_dw -c "SELECT NOW();"

# 4. Daten prüfen (Vergleich UTC vs Berlin)
docker exec train_postgres psql -U postgres -d train_dw -c "SELECT id, ingested_at, ingested_at AT TIME ZONE 'UTC' as as_utc, ingested_at AT TIME ZONE 'Europe/Berlin' as as_berlin FROM stg.db_stations_raw ORDER BY id DESC LIMIT 5;"
```

## Verifikation (CLI)
- Zeilen zählen (Stationsrohdaten):
  - `docker exec train_postgres psql -U dw -d train_dw -c "SELECT COUNT(*) FROM stg.db_stations_raw;"`
- Beispielstationen anzeigen:
  - `docker exec train_postgres psql -U dw -d train_dw -c "SELECT payload->>'name' FROM stg.db_stations_raw ORDER BY id ASC LIMIT 10;"`
- Tabelle leeren vor Neu‑Lauf:
  - `docker exec train_postgres psql -U dw -d train_dw -c "TRUNCATE stg.db_stations_raw;"`

## Verbindungen
- pgAdmin: `http://localhost:8081` (Login aus `.env`), Server: `Host=postgres`, `Port=5432`, `DB=postgres/train_dw`, `User/Pass=postgres/postgres` oder `dw/dw`.
- Lokaler Client: `Host=localhost`, `Port=5433`, `DB=train_dw`, `User=dw`, `Pass=dw`.
