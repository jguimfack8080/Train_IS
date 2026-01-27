-- Vue de fusion pour l'entraînement du modèle LSTM
-- Combine FCHG (Réel), PLAN (Structure), et METEO (Contexte)

CREATE OR REPLACE VIEW dwh.v_training_dataset AS
SELECT
    -- Identifiants et Clés de Jointure
    f.id AS fchg_id,
    f.train_line_ride_id,
    f.eva_number,
    f.station_name,
    f.event_time AS scheduled_time,
    
    -- TARGET : Le retard actuel (à prédire)
    f.delay_in_min AS current_delay,
    
    -- FEATURES FCHG (État dynamique)
    f.train_type,
    f.is_canceled,
    
    -- FEATURES PLAN (Structure théorique)
    p.route_path AS planned_route,
    p.train_category AS planned_category,
    p.platform AS planned_platform,
    
    -- FEATURES METEO (Contexte exogène)
    -- Jointure sur EVA + Heure (arrondie)
    w.temperature_2m,
    w.precipitation,
    w.wind_speed_10m,
    w.weather_code,
    
    -- FEATURES TEMPORELLES (Cycliques)
    EXTRACT(HOUR FROM f.event_time) AS hour_of_day,
    EXTRACT(DOW FROM f.event_time) AS day_of_week

FROM dwh.timetables_fchg_events f
-- Jointure Plan : Récupère les infos structurelles statiques
LEFT JOIN dwh.timetables_plan_events p
    ON f.train_line_ride_id = p.train_line_ride_id
    AND f.station_name = p.station_name
-- Jointure Météo : Récupère la météo à l'heure H sur la gare
LEFT JOIN dwh.v_weather_forecast_hourly w
    ON f.eva_number = w.eva_number
    AND date_trunc('hour', f.event_time) = w.time::timestamptz
WHERE f.event_time IS NOT NULL;
