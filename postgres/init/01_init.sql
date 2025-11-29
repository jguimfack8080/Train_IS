-- ===============================
-- Initiale Einrichtung: Benutzer, Datenbanken, Schemas und Tabellen
-- ===============================

-- Rollen und Datenbank 'airflow' konditional ohne DO/BEGIN (psql \gexec)
SELECT 'CREATE ROLE airflow LOGIN PASSWORD ''airflow''' 
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow');
\gexec

SELECT 'CREATE ROLE dw LOGIN PASSWORD ''dw''' 
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dw');
\gexec

SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow');
\gexec

SELECT 'CREATE DATABASE train_dw OWNER dw'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'train_dw');
\gexec

\connect train_dw
SET ROLE dw;

-- ===============================
-- SCHEMAS
-- ===============================
CREATE SCHEMA IF NOT EXISTS stg AUTHORIZATION dw;
CREATE SCHEMA IF NOT EXISTS psa AUTHORIZATION dw;
CREATE SCHEMA IF NOT EXISTS dwh AUTHORIZATION dw;
CREATE SCHEMA IF NOT EXISTS metadata AUTHORIZATION dw;

-- ===============================
-- PRIVILEGES
-- ===============================
GRANT USAGE, CREATE ON SCHEMA stg TO dw;
GRANT USAGE, CREATE ON SCHEMA psa TO dw;
GRANT USAGE, CREATE ON SCHEMA dwh TO dw;
GRANT USAGE, CREATE ON SCHEMA metadata TO dw;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA stg TO dw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA psa TO dw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dwh TO dw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA metadata TO dw;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA stg TO dw;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA psa TO dw;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA dwh TO dw;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA metadata TO dw;

-- ===============================
-- METADATA Tabellen
-- ===============================
CREATE TABLE IF NOT EXISTS metadata.api_call_log (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  params JSONB,
  status_code INT,
  response_time_ms INT,
  result_count INT,
  called_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS metadata.process_log (
  id BIGSERIAL PRIMARY KEY,
  process_name TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TIMESTAMPTZ DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  message TEXT,
  batch_id TEXT
);

-- ===============================
-- STG Tabellen (Rohdaten) avec noms explicites
-- ===============================
CREATE TABLE IF NOT EXISTS stg.db_stations_raw (
  id BIGSERIAL PRIMARY KEY,
  payload JSONB NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.weather_forecast_raw (
  id BIGSERIAL PRIMARY KEY,
  payload JSONB NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.weather_history_raw (
  id BIGSERIAL PRIMARY KEY,
  payload JSONB NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Timetables (DB Fahrplan) Rohdaten pro Endpoint
CREATE TABLE IF NOT EXISTS stg.timetables_plan_raw (
  id BIGSERIAL PRIMARY KEY,
  payload TEXT NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.timetables_fchg_raw (
  id BIGSERIAL PRIMARY KEY,
  payload TEXT NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.timetables_rchg_raw (
  id BIGSERIAL PRIMARY KEY,
  payload TEXT NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- ===============================
-- PSA‑Tabellen (persistente Rohdaten) mit expliziten Namen
-- ===============================
CREATE TABLE IF NOT EXISTS psa.db_stations_raw (
  id BIGSERIAL PRIMARY KEY,
  payload JSONB NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS psa.weather_forecast_raw (
  id BIGSERIAL PRIMARY KEY,
  payload JSONB NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS psa.weather_history_raw (
  id BIGSERIAL PRIMARY KEY,
  payload JSONB NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Timetables (DB Fahrplan) persistente Rohdaten pro Endpoint
CREATE TABLE IF NOT EXISTS psa.timetables_plan_raw (
  id BIGSERIAL PRIMARY KEY,
  payload TEXT NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS psa.timetables_fchg_raw (
  id BIGSERIAL PRIMARY KEY,
  payload TEXT NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS psa.timetables_rchg_raw (
  id BIGSERIAL PRIMARY KEY,
  payload TEXT NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Vertikalisierte JSON‑Tabellen (STG und DWH)
CREATE TABLE IF NOT EXISTS stg.weather_forecast_vertical_raw (
  id BIGSERIAL PRIMARY KEY,
  payload JSONB NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.weather_history_vertical_raw (
  id BIGSERIAL PRIMARY KEY,
  payload JSONB NOT NULL,
  batch_id TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh.weather_forecast_vertical_raw (
  id BIGSERIAL PRIMARY KEY,
  payload JSONB NOT NULL,
  batch_id TEXT,
  inserted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh.weather_history_vertical_raw (
  id BIGSERIAL PRIMARY KEY,
  payload JSONB NOT NULL,
  batch_id TEXT,
  inserted_at TIMESTAMPTZ DEFAULT NOW()
);

-- Timetables FCHG événements transformés
CREATE TABLE IF NOT EXISTS dwh.timetables_fchg_events (
  id BIGSERIAL PRIMARY KEY,
  eva_number TEXT,
  station_name TEXT,
  train_name TEXT,
  final_destination_station TEXT,
  delay_in_min INT,
  event_time TIMESTAMPTZ,
  is_canceled BOOLEAN,
  type TEXT,
  category TEXT,
  priority INT,
  train_type TEXT,
  train_line_ride_id TEXT,
  train_line_station_num BIGINT,
  arrival_planned_time TIMESTAMPTZ,
  arrival_change_time TIMESTAMPTZ,
  departure_planned_time TIMESTAMPTZ,
  departure_change_time TIMESTAMPTZ,
  message_id TEXT,
  batch_id TEXT,
  inserted_at TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE dwh.timetables_fchg_events IS 'Transformierte Ereignisse aus DB Timetables FCHG (XML) für Analyse/ML; eine Zeile je Ereignis (Meldung oder Verspätungsänderung)';
COMMENT ON COLUMN dwh.timetables_fchg_events.eva_number IS 'EVA-Nummer des Bahnhofs (eindeutiger Stations-Identifikator aus <timetable eva="...">)';
COMMENT ON COLUMN dwh.timetables_fchg_events.station_name IS 'Stationsname aus dem XML-Root (<timetable station="...">)';
COMMENT ON COLUMN dwh.timetables_fchg_events.train_name IS 'Zugname/Nummer aus <tl n="...">';
COMMENT ON COLUMN dwh.timetables_fchg_events.final_destination_station IS 'Letzte Station aus Route (<ppth>/<cpth>)';
COMMENT ON COLUMN dwh.timetables_fchg_events.delay_in_min IS 'Verspätung in Minuten aus <m c="...">';
COMMENT ON COLUMN dwh.timetables_fchg_events.event_time IS 'Zeit des Ereignisses: bevorzugt ct (<ar>/<dp>), sonst ts der Meldung';
COMMENT ON COLUMN dwh.timetables_fchg_events.is_canceled IS 'True wenn t="f" und c>0 in beliebiger Meldung';
COMMENT ON COLUMN dwh.timetables_fchg_events.type IS 'Typ der Meldung (<m t="...">)';
COMMENT ON COLUMN dwh.timetables_fchg_events.category IS 'Kategorie der Meldung (<m cat="...">)';
COMMENT ON COLUMN dwh.timetables_fchg_events.priority IS 'Priorität (<m pr="...">)';
COMMENT ON COLUMN dwh.timetables_fchg_events.train_type IS 'Gattung aus <tl c="...">';
COMMENT ON COLUMN dwh.timetables_fchg_events.train_line_ride_id IS 'Ride-ID aus <s id="...">';
COMMENT ON COLUMN dwh.timetables_fchg_events.train_line_station_num IS 'Liniennummer aus <ar>/<dp l="...">';
COMMENT ON COLUMN dwh.timetables_fchg_events.arrival_planned_time IS 'Geplante Ankunftszeit (<ar pt>)';
COMMENT ON COLUMN dwh.timetables_fchg_events.arrival_change_time IS 'Geänderte Ankunftszeit (<ar ct>)';
COMMENT ON COLUMN dwh.timetables_fchg_events.departure_planned_time IS 'Geplante Abfahrtszeit (<dp pt>)';
COMMENT ON COLUMN dwh.timetables_fchg_events.departure_change_time IS 'Geänderte Abfahrtszeit (<dp ct>)';
COMMENT ON COLUMN dwh.timetables_fchg_events.message_id IS 'Meldungs-ID (<m id="...">)';
COMMENT ON COLUMN dwh.timetables_fchg_events.batch_id IS 'Logische Batch-ID des Pipeline-Laufs (Nachverfolgbarkeit über metadata.process_log)';
COMMENT ON COLUMN dwh.timetables_fchg_events.inserted_at IS 'Zeitpunkt der Einfügung in das DWH (Serverzeit)';

-- ===============================
-- DWH‑View Stationen
-- ===============================
CREATE OR REPLACE VIEW dwh.v_stations AS
WITH base AS (
  SELECT
    r.id,
    (r.payload->>'number')::text AS station_id,
    r.payload->>'name' AS name,
    r.ingested_at AS created_at,
    r.payload
  FROM psa.db_stations_raw r
),
eva AS (
  SELECT
    b.id,
    (e->>'number')::text AS eva_number,
    ((e->'geographicCoordinates'->'coordinates')->>1)::numeric AS latitude,
    ((e->'geographicCoordinates'->'coordinates')->>0)::numeric AS longitude,
    (e->>'isMain')::boolean AS is_main,
    row_number() OVER (
      PARTITION BY b.id
      ORDER BY (e->>'isMain')::boolean DESC NULLS LAST
    ) AS rn
  FROM base b
  LEFT JOIN LATERAL jsonb_array_elements(b.payload->'evaNumbers') e ON true
),
ril AS (
  SELECT
    b.id,
    r->>'rilIdentifier' AS ds100,
    (r->>'isMain')::boolean AS is_main,
    row_number() OVER (
      PARTITION BY b.id
      ORDER BY (r->>'isMain')::boolean DESC NULLS LAST
    ) AS rn
  FROM base b
  LEFT JOIN LATERAL jsonb_array_elements(b.payload->'ril100Identifiers') r ON true
),
eva_one AS (
  SELECT id, eva_number, latitude, longitude
  FROM eva
  WHERE rn = 1
),
ril_one AS (
  SELECT id, ds100
  FROM ril
  WHERE rn = 1
),
assembled AS (
  SELECT
    b.station_id,
    b.name,
    eo.latitude,
    eo.longitude,
    ro.ds100,
    eo.eva_number,
    b.created_at
  FROM base b
  LEFT JOIN eva_one eo ON eo.id = b.id
  LEFT JOIN ril_one ro ON ro.id = b.id
)
SELECT DISTINCT ON (station_id)
  station_id,
  name,
  latitude,
  longitude,
  ds100,
  eva_number,
  created_at
FROM assembled
ORDER BY station_id, created_at DESC;

-- Keine physische Tabelle dwh.weather_history_hourly; nur View

-- ===============================
-- DWH‑Views
-- ===============================
-- View für stündliche Wettervorhersagen
CREATE OR REPLACE VIEW dwh.v_weather_forecast_hourly AS
SELECT DISTINCT ON (s.station_id, r.payload->>'time')
  (r.payload->>'psa_id')::bigint AS psa_id,
  r.batch_id,
  (r.payload->>'latitude')::double precision AS latitude,
  (r.payload->>'longitude')::double precision AS longitude,
  s.station_id,
  s.name AS station_name,
  s.ds100,
  s.eva_number,
  r.payload->>'time' AS time,
  (r.payload->>'temperature_2m')::double precision AS temperature_2m,
  (r.payload->>'relative_humidity_2m')::double precision AS relative_humidity_2m,
  (r.payload->>'dew_point_2m')::double precision AS dew_point_2m,
  (r.payload->>'apparent_temperature')::double precision AS apparent_temperature,
  (r.payload->>'precipitation_probability')::double precision AS precipitation_probability,
  (r.payload->>'precipitation')::double precision AS precipitation,
  (r.payload->>'rain')::double precision AS rain,
  (r.payload->>'showers')::double precision AS showers,
  (r.payload->>'snowfall')::double precision AS snowfall,
  (r.payload->>'snow_depth')::double precision AS snow_depth,
  (r.payload->>'weather_code')::int AS weather_code,
  (r.payload->>'pressure_msl')::double precision AS pressure_msl,
  (r.payload->>'surface_pressure')::double precision AS surface_pressure,
  (r.payload->>'cloud_cover')::double precision AS cloud_cover,
  (r.payload->>'cloud_cover_low')::double precision AS cloud_cover_low,
  (r.payload->>'cloud_cover_mid')::double precision AS cloud_cover_mid,
  (r.payload->>'cloud_cover_high')::double precision AS cloud_cover_high,
  (r.payload->>'visibility')::double precision AS visibility,
  (r.payload->>'wind_speed_10m')::double precision AS wind_speed_10m,
  (r.payload->>'evapotranspiration')::double precision AS evapotranspiration,
  (r.payload->>'vapour_pressure_deficit')::double precision AS vapour_pressure_deficit
FROM dwh.weather_forecast_vertical_raw r
LEFT JOIN dwh.v_stations s ON s.station_id = r.payload->>'station_id'
WHERE (r.payload->>'time') IS NOT NULL
  AND (r.payload->>'station_id') IS NOT NULL
ORDER BY s.station_id, r.payload->>'time', r.batch_id DESC;

-- View für stündliche Wetterhistorie
CREATE OR REPLACE VIEW dwh.v_weather_history_hourly AS
SELECT DISTINCT ON (s.station_id, r.payload->>'time')
  (r.payload->>'psa_id')::bigint AS psa_id,
  r.batch_id,
  (r.payload->>'latitude')::double precision AS latitude,
  (r.payload->>'longitude')::double precision AS longitude,
  s.station_id,
  s.name AS station_name,
  s.ds100,
  s.eva_number,
  r.payload->>'time' AS time,
  (r.payload->>'temperature_2m')::double precision AS temperature_2m,
  (r.payload->>'relative_humidity_2m')::double precision AS relative_humidity_2m,
  (r.payload->>'dew_point_2m')::double precision AS dew_point_2m,
  (r.payload->>'apparent_temperature')::double precision AS apparent_temperature,
  (r.payload->>'precipitation')::double precision AS precipitation,
  (r.payload->>'rain')::double precision AS rain,
  (r.payload->>'snowfall')::double precision AS snowfall,
  (r.payload->>'snow_depth')::double precision AS snow_depth,
  (r.payload->>'weather_code')::int AS weather_code,
  (r.payload->>'pressure_msl')::double precision AS pressure_msl,
  (r.payload->>'surface_pressure')::double precision AS surface_pressure,
  (r.payload->>'cloud_cover')::double precision AS cloud_cover,
  (r.payload->>'cloud_cover_low')::double precision AS cloud_cover_low,
  (r.payload->>'cloud_cover_mid')::double precision AS cloud_cover_mid,
  (r.payload->>'cloud_cover_high')::double precision AS cloud_cover_high,
  (r.payload->>'visibility')::double precision AS visibility,
  (r.payload->>'wind_speed_10m')::double precision AS wind_speed_10m
FROM dwh.weather_history_vertical_raw r
LEFT JOIN dwh.v_stations s ON s.station_id = r.payload->>'station_id'
WHERE (r.payload->>'time') IS NOT NULL
  AND (r.payload->>'station_id') IS NOT NULL
ORDER BY s.station_id, r.payload->>'time', r.batch_id DESC;

-- Empfohlene Indizes zur Performanceoptimierung
CREATE INDEX IF NOT EXISTS idx_forecast_station 
  ON dwh.weather_forecast_vertical_raw ((payload->>'station_id'));

CREATE INDEX IF NOT EXISTS idx_forecast_time 
  ON dwh.weather_forecast_vertical_raw ((payload->>'time'));

CREATE INDEX IF NOT EXISTS idx_history_station 
  ON dwh.weather_history_vertical_raw ((payload->>'station_id'));

CREATE INDEX IF NOT EXISTS idx_history_time 
  ON dwh.weather_history_vertical_raw ((payload->>'time'));

-- ===============================
-- DWH‑View Stationen
-- ===============================
-- (weiter oben verschoben)

-- ===============================
-- Indexes
-- ===============================
CREATE INDEX IF NOT EXISTS idx_stg_db_stations_raw_batch_id ON stg.db_stations_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_psa_db_stations_raw_batch_id ON psa.db_stations_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_weather_forecast_raw_batch_id ON stg.weather_forecast_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_psa_weather_forecast_raw_batch_id ON psa.weather_forecast_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_weather_history_raw_batch_id ON stg.weather_history_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_psa_weather_history_raw_batch_id ON psa.weather_history_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_timetables_plan_raw_batch_id ON stg.timetables_plan_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_psa_timetables_plan_raw_batch_id ON psa.timetables_plan_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_timetables_fchg_raw_batch_id ON stg.timetables_fchg_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_psa_timetables_fchg_raw_batch_id ON psa.timetables_fchg_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_timetables_rchg_raw_batch_id ON stg.timetables_rchg_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_psa_timetables_rchg_raw_batch_id ON psa.timetables_rchg_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_weather_forecast_vertical_raw_batch_id ON stg.weather_forecast_vertical_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_weather_history_vertical_raw_batch_id ON stg.weather_history_vertical_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_dwh_weather_forecast_vertical_raw_batch_id ON dwh.weather_forecast_vertical_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_dwh_weather_history_vertical_raw_batch_id ON dwh.weather_history_vertical_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_dwh_timetables_fchg_events_eva_number ON dwh.timetables_fchg_events(eva_number);
CREATE INDEX IF NOT EXISTS idx_dwh_timetables_fchg_events_time ON dwh.timetables_fchg_events(event_time);
CREATE INDEX IF NOT EXISTS idx_dwh_timetables_fchg_events_category ON dwh.timetables_fchg_events(category);

-- Timetables PLAN événements transformés
CREATE TABLE IF NOT EXISTS dwh.timetables_plan_events (
  id BIGSERIAL PRIMARY KEY,
  station_name TEXT,
  train_line_ride_id TEXT,
  train_number TEXT,
  train_category TEXT,
  train_type TEXT,
  train_direction TEXT,
  event_type TEXT,
  event_time TIMESTAMPTZ,
  platform TEXT,
  train_line_name TEXT,
  route_path TEXT,
  batch_id TEXT,
  inserted_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dwh_timetables_plan_events_station ON dwh.timetables_plan_events(station_name);
CREATE INDEX IF NOT EXISTS idx_dwh_timetables_plan_events_time ON dwh.timetables_plan_events(event_time);

COMMENT ON TABLE dwh.timetables_plan_events IS 'Transformierte Plan-Ereignisse aus DB Timetables PLAN (XML); eine Zeile je Abfahrt/Ankunft';
COMMENT ON COLUMN dwh.timetables_plan_events.station_name IS 'Stationsname aus <timetable station="...">';
COMMENT ON COLUMN dwh.timetables_plan_events.train_line_ride_id IS 'Fahrt-Identifikator/Linienlauf (<s id="...">)';
COMMENT ON COLUMN dwh.timetables_plan_events.train_number IS 'Zugnummer (<tl n="...">)';
COMMENT ON COLUMN dwh.timetables_plan_events.train_category IS 'Zuggattung (z. B. ICE, RE) (<tl c="...">)';
COMMENT ON COLUMN dwh.timetables_plan_events.train_type IS 'Zugtyp/Betriebsart (<tl t="...">)';
COMMENT ON COLUMN dwh.timetables_plan_events.train_direction IS 'Richtungshinweis (<tl f="...">)';
COMMENT ON COLUMN dwh.timetables_plan_events.event_type IS 'Ereignistyp: dp=Abfahrt, ar=Ankunft (Knotentyp)';
COMMENT ON COLUMN dwh.timetables_plan_events.event_time IS 'Planzeit des Ereignisses (pt) als TIMESTAMPTZ';
COMMENT ON COLUMN dwh.timetables_plan_events.platform IS 'Gleis/Bahnsteig (pp)';
COMMENT ON COLUMN dwh.timetables_plan_events.train_line_name IS 'Linienname (l) aus dem Ereignisknoten <dp>/<ar>, z. B. RS1/51';

COMMENT ON COLUMN dwh.timetables_plan_events.route_path IS 'Route/Stationsfolge (ppth)';
COMMENT ON COLUMN dwh.timetables_plan_events.batch_id IS 'Logische Batch-ID des Pipeline-Laufs (Nachverfolgbarkeit)';
COMMENT ON COLUMN dwh.timetables_plan_events.inserted_at IS 'Zeitpunkt der Einfügung in das DWH (Serverzeit)';
-- Timetables RCHG événements transformés
CREATE TABLE IF NOT EXISTS dwh.timetables_rchg_events (
  id BIGSERIAL PRIMARY KEY,
  event_id TEXT,
  eva_number TEXT,
  station_name TEXT,
  message_id TEXT,
  event_type TEXT,
  category TEXT,
  change_type TEXT,
  priority INT,
  valid_from TIMESTAMPTZ,
  valid_to TIMESTAMPTZ,
  old_time TIMESTAMPTZ,
  new_time TIMESTAMPTZ,
  delay_minutes INT,
  platform TEXT,
  platform_change TEXT,
  route TEXT,
  train_line_name TEXT,
  timestamp_event TIMESTAMPTZ,
  batch_id TEXT,
  event_hash TEXT,
  inserted_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_dwh_timetables_rchg_event_hash ON dwh.timetables_rchg_events(event_hash);
CREATE INDEX IF NOT EXISTS idx_dwh_timetables_rchg_events_time ON dwh.timetables_rchg_events(timestamp_event);
CREATE INDEX IF NOT EXISTS idx_dwh_timetables_rchg_events_eva ON dwh.timetables_rchg_events(eva_number);
COMMENT ON TABLE dwh.timetables_rchg_events IS 'Transformierte Ereignisse aus DB Timetables RCHG (Recent Changes, XML); eine Zeile je Nachricht oder Knotenänderung (m/ar/dp) mit normalisierten Zeitstempeln und eindeutiger Deduplikation über event_hash.';
COMMENT ON COLUMN dwh.timetables_rchg_events.event_id IS 'Ereignis-/Knoten‑ID aus <s id="..."> (lokaler Knotenschlüssel im Timetable).';
COMMENT ON COLUMN dwh.timetables_rchg_events.eva_number IS 'EVA‑Nummer des Bahnhofs aus <timetable eva="..."> (eindeutiger Stationsschlüssel).';
COMMENT ON COLUMN dwh.timetables_rchg_events.station_name IS 'Stationsname aus <timetable station="...">.';
COMMENT ON COLUMN dwh.timetables_rchg_events.message_id IS 'Nachrichten‑ID aus <m id="...">; NULL bei Knoten‑Ereignissen ohne Nachricht.';
COMMENT ON COLUMN dwh.timetables_rchg_events.event_type IS 'Ereignistyp: m=Nachricht, dp=Abfahrt, ar=Ankunft.';
COMMENT ON COLUMN dwh.timetables_rchg_events.category IS 'Kategorie der Nachricht (<m cat="...">), z. B. Information, Störung, Bauarbeiten.';
COMMENT ON COLUMN dwh.timetables_rchg_events.change_type IS 'Änderungstyp der Nachricht (<m t="...">), z. B. h/d/etc.';
COMMENT ON COLUMN dwh.timetables_rchg_events.priority IS 'Priorität der Nachricht (<m pr="...">) falls vorhanden.';
COMMENT ON COLUMN dwh.timetables_rchg_events.valid_from IS 'Gültigkeitsbeginn (<m from>) als TIMESTAMPTZ (ISO).';
COMMENT ON COLUMN dwh.timetables_rchg_events.valid_to IS 'Gültigkeitsende (<m to>) als TIMESTAMPTZ (ISO).';
COMMENT ON COLUMN dwh.timetables_rchg_events.old_time IS 'Vorherige Zeit (<dp/ar pt>) als TIMESTAMPTZ (ISO), wenn vorhanden.';
COMMENT ON COLUMN dwh.timetables_rchg_events.new_time IS 'Neue Zeit (<dp/ar ct>) als TIMESTAMPTZ (ISO), wenn vorhanden.';
COMMENT ON COLUMN dwh.timetables_rchg_events.delay_minutes IS 'Verspätung in Minuten (<m c>) falls angegeben.';
COMMENT ON COLUMN dwh.timetables_rchg_events.platform IS 'Gleis/Bahnsteig (<dp/ar pp>) falls vorhanden.';
COMMENT ON COLUMN dwh.timetables_rchg_events.platform_change IS 'Bahnsteigwechsel (<dp/ar cp>) falls vorhanden.';
COMMENT ON COLUMN dwh.timetables_rchg_events.route IS 'Route/Stationsfolge (<dp/ar ppth>) falls vorhanden.';
COMMENT ON COLUMN dwh.timetables_rchg_events.train_line_name IS 'Linienname (<dp/ar l>), z. B. RB12/RE85.';
COMMENT ON COLUMN dwh.timetables_rchg_events.timestamp_event IS 'Ereigniszeit: Präferenz ct (<dp/ar>), sonst ts (<m>), Fallback NOW() UTC (ISO).';
COMMENT ON COLUMN dwh.timetables_rchg_events.batch_id IS 'Logische Batch‑ID des Pipeline‑Laufs (Nachverfolgbarkeit).';
COMMENT ON COLUMN dwh.timetables_rchg_events.event_hash IS 'Deterministischer Hash aus Schlüsselfeldern zur Deduplikation; eindeutiger Index verhindert Duplikate.';
COMMENT ON COLUMN dwh.timetables_rchg_events.inserted_at IS 'Zeitpunkt der Einfügung in das DWH (Serverzeit).';
