import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import os

# DB Connection
DB_USER = os.getenv("DATA_DB_USER", "dw")
DB_PASSWORD = os.getenv("DATA_DB_PASSWORD", "dw")
DB_HOST = os.getenv("DATA_DB_HOST", "postgres")
DB_PORT = os.getenv("DATA_DB_PORT", "5432")
DB_NAME = os.getenv("DATA_DB_NAME", "train_dw")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

def generate_plots():
    print("Fetching data...")
    query = """
    SELECT 
        current_delay,
        temperature_2m,
        precipitation,
        wind_speed_10m,
        hour_of_day
    FROM dwh.v_training_dataset
    WHERE current_delay IS NOT NULL
    LIMIT 10000
    """
    
    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        print(f"Error fetching data: {e}")
        return

    if df.empty:
        print("No data found.")
        return

    # German Labels
    df.rename(columns={
        'current_delay': 'Verspätung (min)',
        'temperature_2m': 'Temperatur (°C)',
        'precipitation': 'Niederschlag (mm)',
        'wind_speed_10m': 'Windgeschw. (km/h)',
        'hour_of_day': 'Stunde'
    }, inplace=True)

    # Plot 1: Correlation Matrix
    plt.figure(figsize=(10, 8))
    # Select only numeric columns for correlation
    numeric_df = df[['Verspätung (min)', 'Temperatur (°C)', 'Niederschlag (mm)', 'Windgeschw. (km/h)']]
    corr = numeric_df.corr()
    sns.heatmap(corr, annot=True, cmap='coolwarm', fmt=".2f", cbar_kws={'label': 'Korrelation'})
    plt.title('Korrelation zwischen Wetter und Verspätungen')
    plt.tight_layout()
    plt.savefig('/app/correlation_matrix.png')
    print("Saved correlation_matrix.png")

    # Plot 2: Average Delay by Hour
    plt.figure(figsize=(10, 6))
    avg_delay = df.groupby('Stunde')['Verspätung (min)'].mean()
    avg_delay.plot(kind='bar', color='skyblue', edgecolor='black')
    plt.title('Durchschnittliche Verspätung nach Tageszeit')
    plt.xlabel('Uhrzeit (Stunde)')
    plt.ylabel('Durchschnittliche Verspätung (Minuten)')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig('/app/delay_hourly.png')
    print("Saved delay_hourly.png")

if __name__ == "__main__":
    generate_plots()
