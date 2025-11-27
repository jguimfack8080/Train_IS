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
- `dwh.weather_hourly` – aufbereitete Wetterzeitreihen.

## Zugriff aus Airflow
- Verbindungsparameter via Umgebungsvariablen `DATA_DB_*`.
- Einfügungen erfolgen als JSONB (Rohpayload) in STG.
- Logs werden in `metadata.api_call_log` geschrieben.
 - Insert‑Logik: Bei Listen wird jeder Eintrag als eigene Zeile eingefügt (Batch‑Insert via `execute_values`).

## Prinzipien
- Keine Code-Duplizierung: Utility-Funktionen für Inserts/Logs.
- Saubere Trennung: STG (roh) → PSA (roh, persistent) → DWH (bereinigt).
- Reproduzierbarkeit: METADATA protokolliert jede Phase und jeden API-Call.

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