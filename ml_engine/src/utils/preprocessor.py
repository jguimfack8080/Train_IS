import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler, LabelEncoder
from sklearn.impute import SimpleImputer
import joblib
import os

class DataPreprocessor:
    def __init__(self, sequence_length=5):
        self.sequence_length = sequence_length
        self.scaler_numerical = RobustScaler()
        self.encoders = {}
        self.imputer = SimpleImputer(strategy='constant', fill_value=0)
        
        self.numerical_cols = [
            'current_delay', 'temperature_2m', 'precipitation', 
            'wind_speed_10m', 'hour_of_day', 'day_of_week'
        ]
        self.categorical_cols = ['train_type', 'station_name']
        
    def fit(self, df: pd.DataFrame):
        # Fit Numerical
        self.scaler_numerical.fit(df[self.numerical_cols])
        
        # Fit Categorical
        for col in self.categorical_cols:
            le = LabelEncoder()
            # Handle unknown classes by converting to string and appending 'Unknown'
            unique_vals = list(df[col].astype(str).unique()) + ['Unknown']
            le.fit(unique_vals)
            self.encoders[col] = le
            
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        # Fill NA
        df[self.numerical_cols] = self.imputer.fit_transform(df[self.numerical_cols]) # Use fit_transform just for filling, technically should be transform but simple imputer is stateless for constant
        
        # Scale Numerical
        df[self.numerical_cols] = self.scaler_numerical.transform(df[self.numerical_cols])
        
        # Encode Categorical
        for col in self.categorical_cols:
            le = self.encoders[col]
            df[col] = df[col].astype(str).apply(lambda x: x if x in le.classes_ else 'Unknown')
            df[col] = le.transform(df[col])
            
        return df

    def create_sequences(self, df: pd.DataFrame, target_col='current_delay'):
        """
        Génère des séquences (Samples, TimeSteps, Features)
        Groupe par 'train_line_ride_id' pour respecter la continuité temporelle d'un trajet.
        """
        sequences = []
        targets = []
        ids = [] # To keep track of which ride/station we predict
        
        # Features list order must be consistent
        feature_cols = self.numerical_cols + self.categorical_cols
        
        grouped = df.groupby('train_line_ride_id')
        
        for ride_id, group in grouped:
            group = group.sort_values('scheduled_time')
            data = group[feature_cols].values
            target = group[target_col].values
            meta = group[['train_line_ride_id', 'eva_number', 'scheduled_time']].values
            
            if len(data) <= self.sequence_length:
                continue
                
            for i in range(len(data) - self.sequence_length):
                seq = data[i:(i + self.sequence_length)]
                label = target[i + self.sequence_length]
                meta_info = meta[i + self.sequence_length]
                
                sequences.append(seq)
                targets.append(label)
                ids.append(meta_info)
                
        return np.array(sequences), np.array(targets), np.array(ids)

    def save(self, path='artifacts'):
        os.makedirs(path, exist_ok=True)
        joblib.dump(self.scaler_numerical, os.path.join(path, 'scaler.joblib'))
        joblib.dump(self.encoders, os.path.join(path, 'encoders.joblib'))

    def load(self, path='artifacts'):
        self.scaler_numerical = joblib.load(os.path.join(path, 'scaler.joblib'))
        self.encoders = joblib.load(os.path.join(path, 'encoders.joblib'))
