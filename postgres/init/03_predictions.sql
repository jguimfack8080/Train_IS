-- Table pour stocker les prédictions du modèle LSTM
CREATE TABLE IF NOT EXISTS dwh.predictions (
    id BIGSERIAL PRIMARY KEY,
    train_line_ride_id TEXT NOT NULL,
    eva_number TEXT NOT NULL,
    station_name TEXT,
    scheduled_time TIMESTAMPTZ NOT NULL,
    predicted_delay_min NUMERIC(5,2),
    prediction_proba_class TEXT, -- 'NO_DELAY', 'POSSIBLE', 'CRITICAL'
    confidence_score NUMERIC(3,2),
    model_version TEXT,
    predicted_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Contrainte d'unicité pour éviter les doublons sur une même prédiction
    UNIQUE(train_line_ride_id, eva_number, scheduled_time, model_version)
);

CREATE INDEX IF NOT EXISTS idx_predictions_ride_id ON dwh.predictions(train_line_ride_id);
CREATE INDEX IF NOT EXISTS idx_predictions_time ON dwh.predictions(scheduled_time);
