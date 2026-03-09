import os
import gc
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import numpy as np
import argparse
from datetime import datetime, timedelta

from utils.db import load_data
from utils.preprocessor import DataPreprocessor
from model import TrainDelayLSTM

# Hyperparams
BATCH_SIZE = 512
EPOCHS = 5  # Reduced for frequent retraining
LEARNING_RATE = 0.001
HIDDEN_DIM = 64
NUM_LAYERS = 2
SEQ_LENGTH = 5

def train(hours=24):
    print(f"🚀 Démarrage de l'entraînement (Fenêtre: {hours}h)...")
    
    # 1. Load Data
    start_date = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
    print(f"📥 Chargement des données depuis {start_date}...")
    
    # Load data from DWH view
    query = f"""
        SELECT * 
        FROM dwh.v_training_dataset 
        WHERE scheduled_time >= '{start_date}'
    """
    df = load_data(query) 
    
    if df.empty:
        print("⚠️ Aucune donnée trouvée pour cette période !")
        return

    # 2. Preprocess
    print(f"⚙️ Préparation des features ({len(df)} lignes)...")
    preprocessor = DataPreprocessor(sequence_length=SEQ_LENGTH)
    preprocessor.fit(df)
    
    # Transform and optimize memory
    df_transformed = preprocessor.transform(df)
    
    # MEMORY OPTIMIZATION: Delete original dataframe
    del df
    gc.collect()
    
    # 3. Create Sequences
    print("✂️ Création des séquences temporelles...")
    X, y, _ = preprocessor.create_sequences(df_transformed)
    
    del df_transformed
    gc.collect()
    
    if len(X) == 0:
        print("⚠️ Pas assez de données pour créer des séquences.")
        return

    # Save input dimensions
    input_dim = X.shape[2]

    # Split Train/Val
    split_idx = int(len(X) * 0.8)
    X_train, X_val = X[:split_idx], X[split_idx:]
    y_train, y_val = y[:split_idx], y[split_idx:]
    
    del X
    del y
    gc.collect()
    
    # Convert to PyTorch Tensors
    train_data = TensorDataset(torch.tensor(X_train, dtype=torch.float32), torch.tensor(y_train, dtype=torch.float32))
    val_data = TensorDataset(torch.tensor(X_val, dtype=torch.float32), torch.tensor(y_val, dtype=torch.float32))
    
    del X_train, X_val, y_train, y_val
    gc.collect()
    
    train_loader = DataLoader(train_data, shuffle=True, batch_size=BATCH_SIZE)
    val_loader = DataLoader(val_data, batch_size=BATCH_SIZE)
    
    # 4. Initialize Model
    model = TrainDelayLSTM(input_dim, HIDDEN_DIM, NUM_LAYERS)
    
    # Load existing model if available for incremental learning (optional, but good for stability)
    # model_path = "artifacts/model.pth"
    # if os.path.exists(model_path):
    #     try:
    #         model.load_state_dict(torch.load(model_path))
    #         print("🔄 Modèle existant chargé pour fine-tuning.")
    #     except:
    #         print("⚠️ Impossible de charger l'ancien modèle, entraînement à partir de zéro.")

    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)
    
    # 5. Training Loop
    print("🏋️ Début des époques...")
    best_val_loss = float('inf')
    
    for epoch in range(EPOCHS):
        model.train()
        train_loss = 0
        for X_batch, y_batch in train_loader:
            optimizer.zero_grad()
            outputs = model(X_batch)
            loss = criterion(outputs.squeeze(), y_batch)
            loss.backward()
            optimizer.step()
            train_loss += loss.item()
            
        # Validation
        model.eval()
        val_loss = 0
        with torch.no_grad():
            for X_batch, y_batch in val_loader:
                outputs = model(X_batch)
                loss = criterion(outputs.squeeze(), y_batch)
                val_loss += loss.item()
        
        avg_train_loss = train_loss / len(train_loader)
        avg_val_loss = val_loss / len(val_loader)
        
        print(f"Epoch {epoch+1}/{EPOCHS} | Train Loss: {avg_train_loss:.4f} | Val Loss: {avg_val_loss:.4f}")
        
        # Save Best Model
        if avg_val_loss < best_val_loss:
            best_val_loss = avg_val_loss
            os.makedirs("artifacts", exist_ok=True)
            torch.save(model.state_dict(), "artifacts/model.pth")
            preprocessor.save("artifacts")
            print("💾 Modèle sauvegardé.")

    # 6. Save Model
    print("💾 Sauvegarde du modèle...")
    if not os.path.exists("artifacts"):
        os.makedirs("artifacts")
        
    torch.save(model.state_dict(), "artifacts/model.pth")
    preprocessor.save("artifacts")
    print("✅ Modèle sauvegardé avec succès.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=24, help="Training data window in hours")
    args = parser.parse_args()
    
    train(hours=args.hours)
