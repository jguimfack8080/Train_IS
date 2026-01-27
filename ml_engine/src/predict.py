import os
import torch
import pandas as pd
import numpy as np
from datetime import datetime

from utils.db import DBConnection
from utils.preprocessor import DataPreprocessor
from model import TrainDelayLSTM

# Config
HIDDEN_DIM = 64
NUM_LAYERS = 2
SEQ_LENGTH = 5

def predict():
    print("🔮 Démarrage de l'inférence...")
    
    # 1. Load Artifacts
    if not os.path.exists("artifacts/model.pth"):
        print("⚠️ Modèle non trouvé. Veuillez entraîner le modèle d'abord.")
        return

    preprocessor = DataPreprocessor(sequence_length=SEQ_LENGTH)
    try:
        preprocessor.load("artifacts")
    except:
        print("⚠️ Scalers non trouvés.")
        return

    # 2. Load Data (Recent data for prediction)
    db = DBConnection()
    # On filtre pour ne prendre que les trains récents (ex: depuis 4h) pour éviter de reprédire le passé
    # On supprime la limite (limit=None) pour s'assurer de capturer TOUT le trafic de la fenêtre temporelle
    start_window = (datetime.now() - pd.Timedelta(hours=4)).strftime('%Y-%m-%d %H:%M:%S')
    print(f"📡 Récupération des données depuis {start_window}...")
    df = db.get_training_data(limit=None, start_date=start_window) 
    
    if df.empty:
        print("⚠️ Pas de données récentes (vérifiez le pipeline d'ingestion).")
        return

    # 3. Transform
    df_transformed = preprocessor.transform(df)
    
    # 4. Create Sequences
    # Note: En prod, on ne prédirait que le dernier point de chaque train. 
    # Ici, pour la démo, on prédit sur tout ce qu'on peut.
    X, _, ids = preprocessor.create_sequences(df_transformed, target_col='current_delay')
    
    if len(X) == 0:
        print("⚠️ Pas de séquences générées.")
        return

    # 5. Load Model
    input_dim = X.shape[2]
    model = TrainDelayLSTM(input_dim, HIDDEN_DIM, NUM_LAYERS)
    model.load_state_dict(torch.load("artifacts/model.pth"))
    model.eval()
    
    # 6. Predict
    with torch.no_grad():
        inputs = torch.Tensor(X)
        outputs = model(inputs).squeeze().numpy()
        
    # 7. Format Results
    results = []
    timestamp = datetime.now()
    
    # Inverse scaling pour récupérer la vraie valeur (approximation simple ici car on a scalé tout le dataset)
    # Pour faire propre, il faudrait utiliser inverse_transform sur la colonne cible uniquement.
    # Ici on suppose que le modèle sort une valeur normalisée, il faut la dénormaliser.
    # Hack rapide : On utilise le scale_ du RobustScaler pour la colonne delay (index 0)
    delay_scale = preprocessor.scaler_numerical.scale_[0]
    delay_center = preprocessor.scaler_numerical.center_[0]
    
    predictions_min = (outputs * delay_scale) + delay_center
    
    for i in range(len(ids)):
        ride_id, eva, sched_time = ids[i]
        pred_val = float(predictions_min[i])
        
        # Classification Rule
        if pred_val < 5:
            cat = 'NO_DELAY'
        elif pred_val < 15:
            cat = 'POSSIBLE'
        else:
            cat = 'CRITICAL'
            
        results.append({
            'train_line_ride_id': ride_id,
            'eva_number': eva,
            'station_name': 'Unknown', # Pas dans ids pour simplifier
            'scheduled_time': sched_time,
            'predicted_delay_min': round(pred_val, 2),
            'prediction_proba_class': cat,
            'confidence_score': 0.95, # Dummy score
            'model_version': 'v1.0',
            'predicted_at': timestamp
        })
        
    # 8. Save to DB
    df_results = pd.DataFrame(results)
    print(f"💾 Sauvegarde de {len(df_results)} prédictions...")
    
    # Simple Loop Fallback for Safety
    try:
        # Try chunked insert first
        df_results.to_sql('predictions', db.engine, schema='dwh', if_exists='append', index=False, method='multi', chunksize=50)
    except Exception as e:
        print(f"⚠️ Erreur insertion chunkée : {e}")
        print("🔄 Tentative d'insertion ligne par ligne...")
        for i, row in df_results.iterrows():
            try:
                pd.DataFrame([row]).to_sql('predictions', db.engine, schema='dwh', if_exists='append', index=False, method='multi')
            except Exception as inner_e:
                # Ignore duplicates or single errors
                pass
    
    print("✅ Inférence terminée.")

if __name__ == "__main__":
    predict()
