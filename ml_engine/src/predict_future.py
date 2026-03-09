import os
import sys
import pandas as pd
import numpy as np
import torch
import joblib
from datetime import datetime, timedelta, timezone
from sqlalchemy import text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert

# Add src to path to import local modules
sys.path.append(os.path.join(os.path.dirname(__file__)))

from utils.db import get_db_engine, load_data
from utils.preprocessor import DataPreprocessor
from model import TrainDelayLSTM

def predict_future_delays():
    print("🚀 Démarrage de la génération des prédictions futures...")
    
    # 1. Load Model and Artifacts
    print("📦 Chargement du modèle et des artefacts...")
    artifacts_path = os.path.join(os.path.dirname(__file__), "artifacts")
    
    # Check if artifacts exist
    if not os.path.exists(os.path.join(artifacts_path, "model.pth")):
        print(f"❌ Erreur: Modèle non trouvé dans {artifacts_path}")
        return

    # Load preprocessor (scaler/encoders)
    preprocessor = DataPreprocessor()
    try:
        preprocessor.load(artifacts_path)
    except Exception as e:
        print(f"❌ Erreur lors du chargement du préprocesseur: {e}")
        return

    # Load PyTorch model
    input_dim = len(preprocessor.numerical_cols) + len(preprocessor.categorical_cols)
    hidden_dim = 64 # Assuming these match training
    num_layers = 2
    
    model = TrainDelayLSTM(input_dim, hidden_dim, num_layers)
    model.load_state_dict(torch.load(os.path.join(artifacts_path, "model.pth")))
    model.eval()
    
    # 2. Fetch Future Schedule & Weather Data
    print("📅 Récupération des horaires futurs et de la météo...")
    
    # Query to join plan events with weather forecast
    # We take plan events from NOW() to NOW() + 24h
    # We join with weather on station and time (rounded to hour)
    query = """
    WITH future_trains AS (
        SELECT 
            t.train_line_ride_id,
            -- t.eva_number, -- Not in plan events, will join later or ignore
            t.station_name,
            t.event_time as scheduled_time,
            t.train_type,
            EXTRACT(HOUR FROM t.event_time) as hour_of_day,
            EXTRACT(DOW FROM t.event_time) as day_of_week
        FROM dwh.timetables_plan_events t
        WHERE t.event_time >= NOW() 
          AND t.event_time <= NOW() + INTERVAL '24 HOURS'
    ),
    weather AS (
        SELECT 
            w.station_name,
            w.time::timestamp as weather_time,
            w.temperature_2m,
            w.precipitation,
            w.wind_speed_10m
        FROM dwh.v_weather_forecast_hourly w
        WHERE w.time::timestamp >= DATE_TRUNC('hour', NOW())
    ),
    stations AS (
        SELECT DISTINCT name as station_name, eva_number FROM dwh.v_stations
    )
    SELECT 
        ft.*,
        COALESCE(s.eva_number, 'Unknown') as eva_number,
        COALESCE(w.temperature_2m, 15.0) as temperature_2m,
        COALESCE(w.precipitation, 0.0) as precipitation,
        COALESCE(w.wind_speed_10m, 10.0) as wind_speed_10m
    FROM future_trains ft
    LEFT JOIN weather w 
    ON ft.station_name = w.station_name 
    AND DATE_TRUNC('hour', ft.scheduled_time) = w.weather_time
    LEFT JOIN stations s ON ft.station_name = s.station_name
    """
    
    df = load_data(query)
    
    if df.empty:
        print("⚠️ Aucune donnée future trouvée (horaires ou météo manquants).")
        return
        
    print(f"✅ {len(df)} trains futurs récupérés.")
    
    # 3. Prepare Data for Inference
    print("⚙️ Préparation des données pour l'inférence...")
    
    # Add 'current_delay' = 0 (Assumption for future scheduled trains)
    df['current_delay'] = 0.0
    
    # Transform using preprocessor
    # Note: We need to handle 'Unknown' categories gracefully
    try:
        df_processed = preprocessor.transform(df)
    except Exception as e:
        print(f"⚠️ Erreur lors de la transformation: {e}")
        # Fallback: re-fit encoders if necessary or skip
        return

    # Create sequences
    # Since we want to predict for specific future points without history, 
    # we will replicate the static features to create a "steady state" sequence.
    # This assumes the condition persists for the sequence length.
    sequence_length = 5
    
    features_cols = preprocessor.numerical_cols + preprocessor.categorical_cols
    feature_data = df_processed[features_cols].values
    
    # Create batch of sequences: (N, seq_len, features)
    # We repeat the same feature vector seq_len times for each sample
    sequences = np.tile(feature_data[:, np.newaxis, :], (1, sequence_length, 1))
    
    # Convert to Tensor
    X_tensor = torch.FloatTensor(sequences)
    
    # 4. Run Inference
    print("🔮 Génération des prédictions...")
    with torch.no_grad():
        predictions = model(X_tensor)
        predicted_delays = predictions.numpy().flatten()
        
    # Add predictions back to dataframe
    df['predicted_delay_min'] = predicted_delays
    
    # Check for NaN or Inf
    if df['predicted_delay_min'].isnull().any() or np.isinf(df['predicted_delay_min']).any():
        print("⚠️ Attention: NaN ou Inf détectés dans les prédictions. Remplacement par 0.")
        df['predicted_delay_min'] = df['predicted_delay_min'].replace([np.inf, -np.inf], 0).fillna(0)

    # Calculate confidence/proba (heuristic based on delay magnitude)
    # Simple sigmoid-like mapping or categorization
    df['prediction_proba_class'] = df['predicted_delay_min'].apply(
        lambda x: 'High' if x > 5 else ('Medium' if x > 2 else 'Low')
    )
    df['confidence_score'] = 0.8 # Static confidence for now since we lack history
    
    # 5. Save to Database
    print("💾 Sauvegarde en base de données (Upsert)...")
    
    # Prepare output dataframe matching dwh.predictions schema
    output_df = pd.DataFrame({
        'train_line_ride_id': df['train_line_ride_id'],
        'eva_number': df['eva_number'],
        'station_name': df['station_name'],
        'scheduled_time': df['scheduled_time'],
        'predicted_delay_min': df['predicted_delay_min'].astype(float),
        'prediction_proba_class': df['prediction_proba_class'],
        'confidence_score': df['confidence_score'].astype(float),
        'model_version': 'v1.0.0',
        'predicted_at': datetime.now(timezone.utc)
    })
    
    # Drop duplicates to avoid "ON CONFLICT DO UPDATE command cannot affect row a second time"
    output_df = output_df.drop_duplicates(subset=['train_line_ride_id', 'eva_number', 'scheduled_time', 'model_version'])
    
    engine = get_db_engine()
    
    # Convert DataFrame to list of dicts for SQLAlchemy
    data_to_insert = output_df.to_dict(orient='records')
    
    if not data_to_insert:
        print("⚠️ Aucune prédiction à sauvegarder.")
        return

    try:
        metadata = MetaData()
        # Reflect table to get column objects
        predictions_table = Table('predictions', metadata, schema='dwh', autoload_with=engine)
        
        # Prepare the insert statement
        stmt = insert(predictions_table).values(data_to_insert)
        
        # Prepare the update dict for ON CONFLICT DO UPDATE
        # We update the predicted delay and metadata if the key exists
        update_dict = {
            'predicted_delay_min': stmt.excluded.predicted_delay_min,
            'prediction_proba_class': stmt.excluded.prediction_proba_class,
            'confidence_score': stmt.excluded.confidence_score,
            'predicted_at': stmt.excluded.predicted_at,
            # Also update station info just in case it changed (unlikely for same ID/time)
            'station_name': stmt.excluded.station_name
        }
        
        # Construct the full upsert statement
        # Using index_elements for the unique constraint columns
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=['train_line_ride_id', 'eva_number', 'scheduled_time', 'model_version'],
            set_=update_dict
        )
        
        # Execute in a transaction
        with engine.begin() as conn:
            result = conn.execute(upsert_stmt)
            print(f"✅ {result.rowcount} prédictions sauvegardées/mises à jour avec succès.")
            
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde: {e}")

if __name__ == "__main__":
    predict_future_delays()
