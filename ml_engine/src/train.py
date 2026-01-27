import os
import gc
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import numpy as np

from utils.db import DBConnection
from utils.preprocessor import DataPreprocessor
from model import TrainDelayLSTM

# Hyperparams
BATCH_SIZE = 512  # Increased from 64 to speed up training on large dataset
EPOCHS = 10
LEARNING_RATE = 0.001
HIDDEN_DIM = 64
NUM_LAYERS = 2
SEQ_LENGTH = 5

def train():
    print("🚀 Démarrage de l'entraînement...")
    
    # 1. Load Data
    db = DBConnection()
    print("📥 Chargement des données depuis DWH (Dataset complet)...")
    # Note: Loading full dataset might cause OOM (Exit Code 137) on limited RAM environments.
    # If this crashes, consider re-introducing a limit (e.g., limit=50000).
    df = db.get_training_data(limit=None) 
    
    if df.empty:
        print("⚠️ Aucune donnée trouvée !")
        return

    # 2. Preprocess
    print("⚙️ Préparation des features...")
    preprocessor = DataPreprocessor(sequence_length=SEQ_LENGTH)
    preprocessor.fit(df)
    
    # Transform and optimize memory
    df_transformed = preprocessor.transform(df)
    
    # MEMORY OPTIMIZATION: Delete original dataframe to free RAM
    del df
    gc.collect()
    print("🧹 Mémoire libérée (Raw Data).")
    
    # 3. Create Sequences
    print("✂️ Création des séquences temporelles...")
    # This step expands data by factor of SEQ_LENGTH (sliding window)
    X, y, _ = preprocessor.create_sequences(df_transformed)
    
    # MEMORY OPTIMIZATION: Delete transformed dataframe
    del df_transformed
    gc.collect()
    print("🧹 Mémoire libérée (Transformed Data).")
    
    if len(X) == 0:
        print("⚠️ Pas assez de données pour créer des séquences.")
        return

    # Save input dimensions before deleting X
    input_dim = X.shape[2]

    # Split Train/Val
    split_idx = int(len(X) * 0.8)
    X_train, X_val = X[:split_idx], X[split_idx:]
    y_train, y_val = y[:split_idx], y[split_idx:]
    
    # MEMORY OPTIMIZATION: Delete full X, y arrays after splitting (if possible)
    # Actually, we need to keep splits. But we can delete X and y references.
    del X
    del y
    gc.collect()
    
    # Convert to PyTorch Tensors
    # Using float32 to save memory (default is often float64 for numpy)
    train_data = TensorDataset(torch.tensor(X_train, dtype=torch.float32), torch.tensor(y_train, dtype=torch.float32))
    val_data = TensorDataset(torch.tensor(X_val, dtype=torch.float32), torch.tensor(y_val, dtype=torch.float32))
    
    # MEMORY OPTIMIZATION: Delete numpy arrays after tensor conversion
    del X_train, X_val, y_train, y_val
    gc.collect()
    print("🧹 Mémoire libérée (Numpy Arrays).")
    
    train_loader = DataLoader(train_data, shuffle=True, batch_size=BATCH_SIZE)
    val_loader = DataLoader(val_data, batch_size=BATCH_SIZE)
    
    # 4. Initialize Model
    # input_dim is already saved
    model = TrainDelayLSTM(input_dim, HIDDEN_DIM, NUM_LAYERS)
    
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

    print("✅ Entraînement terminé.")

if __name__ == "__main__":
    train()
