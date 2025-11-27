# Docker / Compose

## Komponenten
- `postgres` – Datenbank mit `train_dw` (DWH) und `airflow` (Airflow-Metadaten).
- `airflow-webserver` – Airflow Web UI.
- `airflow-scheduler` – Ausführung der DAGs.
 - `pgadmin` – Web-Oberfläche für PostgreSQL zur Administration und Abfrage.

## Nutzung
1. `.env.example` nach `.env` kopieren und Variablen setzen.
2. Build und Start:
   - `docker compose up -d --build`
3. Stoppen:
   - `docker compose down` (Datenvolumes bleiben erhalten)
4. Neuaufsetzen (inkl. Datenlöschung):
   - Windows PowerShell: Befehle getrennt ausführen (kein `&&`)
     - `docker compose down -v`
     - `docker compose up -d --build`

## Volumes
- `pgdata` – persistente Daten des Postgres-Containers.
- Mounts für Airflow: `airflow/dags`, `airflow/include`, `airflow/requirements.txt`.
 - `pgadmin_data` – persistente Daten von pgAdmin (Server-Definitionen, Einstellungen).

## Hinweise
- Auf Windows wird Docker Desktop benötigt.
- Airflow-Nutzer wird beim ersten Start erstellt (`admin`/`admin`).
- Die Installation von Python-Abhängigkeiten erfolgt beim Containerstart.
 - Zugriff auf pgAdmin: `http://localhost:8081` (Login aus `.env`: `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD`).
   - In pgAdmin neuen Server anlegen: Name frei, Host: `postgres`, Port: `5432`, Maintenance DB: `postgres` (oder `train_dw` nach Init), Benutzer: `postgres` (oder `dw`), Passwort: aus `.env`.
- Ports: Airflow Web UI `http://localhost:8085`, Postgres lokal `localhost:5433` (weitergeleitet auf Containerport `5432`).
- Postgres Init: `POSTGRES_DB=postgres`; die Fach-Datenbank `train_dw` wird durch `postgres/init/01_init.sql` mit Owner `dw` erstellt (kein `ALTER`).
 - DWH: Ereignistabelle `dwh.timetables_fchg_events` (EVA‑basiert) wird durch den FCHG‑Import‑DAG befüllt.
