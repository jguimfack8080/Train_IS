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
  event_time TIMESTAMPTZ,
  message_id TEXT,
  type TEXT,
  category TEXT,
  priority INT,
  delay_minutes INT,
  valid_from TIMESTAMPTZ,
  valid_to TIMESTAMPTZ,
  platform_change TEXT,
  batch_id TEXT,
  inserted_at TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE dwh.timetables_fchg_events IS 'Transformierte Ereignisse aus DB Timetables FCHG (XML) für Analyse/ML; eine Zeile je Ereignis (Meldung oder Verspätungsänderung)';
COMMENT ON COLUMN dwh.timetables_fchg_events.eva_number IS 'EVA-Nummer des Bahnhofs (eindeutiger Stations-Identifikator aus <timetable eva="...">)';
COMMENT ON COLUMN dwh.timetables_fchg_events.station_name IS 'Stationsname aus dem XML-Root (<timetable station="...">)';
COMMENT ON COLUMN dwh.timetables_fchg_events.event_time IS 'Ereigniszeitpunkt: bevorzugt ct des Elternknotens (<ar>/<dp>), sonst ts der Meldung (<m>); als TIMESTAMPTZ gespeichert';
COMMENT ON COLUMN dwh.timetables_fchg_events.message_id IS 'ID der Meldung (<m id="...">), eindeutiger Nachrichten-Identifikator';
COMMENT ON COLUMN dwh.timetables_fchg_events.type IS 'Typ des Ereignisses: d=Abfahrt, f=Ankunft, h=Hinweis/Info (wie im Attribut <m t="...">)';
COMMENT ON COLUMN dwh.timetables_fchg_events.category IS 'Kategorie der Meldung (z. B. Störung, Information); nur für <m> auf Ebene <s> vorhanden';
COMMENT ON COLUMN dwh.timetables_fchg_events.priority IS 'Priorität der Meldung (pr), falls angegeben';
COMMENT ON COLUMN dwh.timetables_fchg_events.delay_minutes IS 'Verspätungscode in Minuten (c) aus Meldungen unter <ar>/<dp>';
COMMENT ON COLUMN dwh.timetables_fchg_events.valid_from IS 'Beginn der Gültigkeit (from) einer Hinweis-/Störungsmeldung; TIMESTAMPTZ';
COMMENT ON COLUMN dwh.timetables_fchg_events.valid_to IS 'Ende der Gültigkeit (to) einer Hinweis-/Störungsmeldung; TIMESTAMPTZ';
COMMENT ON COLUMN dwh.timetables_fchg_events.platform_change IS 'Bahnsteig/Gleisänderung (cp) aus <ar>/<dp>, falls vorhanden';
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
  service_id TEXT,
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
COMMENT ON COLUMN dwh.timetables_plan_events.service_id IS 'Dienst-/Fahrt-Identifikator (<s id="...">)';
COMMENT ON COLUMN dwh.timetables_plan_events.train_number IS 'Zugnummer (<tl n="...">)';
COMMENT ON COLUMN dwh.timetables_plan_events.train_category IS 'Zuggattung (z. B. ICE, RE) (<tl c="...">)';
COMMENT ON COLUMN dwh.timetables_plan_events.train_type IS 'Zugtyp/Betriebsart (<tl t="...">)';
COMMENT ON COLUMN dwh.timetables_plan_events.train_direction IS 'Richtungshinweis (<tl f="...">)';
COMMENT ON COLUMN dwh.timetables_plan_events.event_type IS 'Ereignistyp: dp=Abfahrt, ar=Ankunft (Knotentyp)';
COMMENT ON COLUMN dwh.timetables_plan_events.event_time IS 'Planzeit des Ereignisses (pt) als TIMESTAMPTZ';
COMMENT ON COLUMN dwh.timetables_plan_events.platform IS 'Gleis/Bahnsteig (pp)';

COMMENT ON COLUMN dwh.timetables_plan_events.route_path IS 'Route/Stationsfolge (ppth)';
COMMENT ON COLUMN dwh.timetables_plan_events.batch_id IS 'Logische Batch-ID des Pipeline-Laufs (Nachverfolgbarkeit)';
COMMENT ON COLUMN dwh.timetables_plan_events.inserted_at IS 'Zeitpunkt der Einfügung in das DWH (Serverzeit)';
