import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os

# Page Config
st.set_page_config(
    page_title="Train Delay Prediction - Dashboard",
    page_icon="🚄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Modern Look
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* Global Cards */
    .kpi-card {
        background: white;
        border-radius: 12px;
        padding: 20px 24px;
        margin: 10px 0;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        border: 1px solid #e5e7eb;
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    
    .kpi-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
    }
    
    .kpi-title {
        font-size: 0.85rem;
        color: #6B7280;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 600;
        margin-bottom: 8px;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        color: #111827;
        line-height: 1.2;
    }
    
    .kpi-icon {
        font-size: 1.2rem;
        padding: 6px;
        border-radius: 8px;
        background: rgba(0,0,0,0.05);
    }

    /* Risk Specifics - Elegant Accents */
    .card-critical {
        border-left: 5px solid #EF4444;
        background: linear-gradient(to right, #FEF2F2, #FFFFFF);
    }
    .card-critical .kpi-value { color: #B91C1C; }
    .card-critical .kpi-icon { color: #EF4444; background: #FEE2E2; }

    .card-possible {
        border-left: 5px solid #F59E0B;
        background: linear-gradient(to right, #FFFBEB, #FFFFFF);
    }
    .card-possible .kpi-value { color: #B45309; }
    .card-possible .kpi-icon { color: #F59E0B; background: #FEF3C7; }

    .card-ok {
        border-left: 5px solid #10B981;
        background: linear-gradient(to right, #ECFDF5, #FFFFFF);
    }
    .card-ok .kpi-value { color: #047857; }
    .card-ok .kpi-icon { color: #10B981; background: #D1FAE5; }
    
    .card-neutral {
        border-left: 5px solid #6B7280;
        background: white;
    }
</style>
""", unsafe_allow_html=True)

# Database Connection
@st.cache_resource
def get_engine():
    db_user = os.getenv("DATA_DB_USER", "dw")
    db_password = os.getenv("DATA_DB_PASSWORD", "dw")
    db_host = os.getenv("DATA_DB_HOST", "postgres")
    db_port = os.getenv("DATA_DB_PORT", "5432")
    db_name = os.getenv("DATA_DB_NAME", "train_dw")
    
    connection_str = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(connection_str)

@st.cache_data(ttl=600)
def load_data():
    engine = get_engine()
    # Limit to last 1000 records for performance in MVP
    query = """
    SELECT * 
    FROM dwh.v_training_dataset 
    ORDER BY scheduled_time DESC 
    LIMIT 2000;
    """
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données: {e}")
        return pd.DataFrame()

# Main App
def main():
    st.title("🚄 Bremen Train Delay Intelligence")
    st.markdown("### Analyse et Monitoring des Retards (Dataset ML)")

    # Sidebar
    st.sidebar.header("Filtres")
    
    # Load Data
    with st.spinner('Chargement des données...'):
        df = load_data()

    if df.empty:
        st.warning("Aucune donnée disponible ou erreur de connexion.")
        return

    # Sidebar Filters
    stations = ["Toutes"] + list(df['station_name'].dropna().unique())
    selected_station = st.sidebar.selectbox("Gare", stations)
    
    train_types = ["Tous"] + list(df['train_type'].dropna().unique())
    selected_type = st.sidebar.selectbox("Type de Train", train_types)

    # Filtering Logic
    filtered_df = df.copy()
    if selected_station != "Toutes":
        filtered_df = filtered_df[filtered_df['station_name'] == selected_station]
    if selected_type != "Tous":
        filtered_df = filtered_df[filtered_df['train_type'] == selected_type]

    # KPIs
    st.markdown("### 📊 Performance Historique (24h)")
    col1, col2, col3, col4 = st.columns(4)
    
    avg_delay = filtered_df['current_delay'].mean()
    max_delay = filtered_df['current_delay'].max()
    cancel_rate = filtered_df['is_canceled'].mean() * 100
    weather_impact = filtered_df[filtered_df['precipitation'] > 0]['current_delay'].mean()

    # Helper for KPI Card
    def kpi_card(title, value, icon, col, status="neutral"):
        card_class = "kpi-card"
        if status == "critical": card_class += " card-critical"
        elif status == "warning": card_class += " card-possible"
        elif status == "good": card_class += " card-ok"
        
        col.markdown(f"""
        <div class="{card_class}">
            <div class="kpi-title">{icon} {title}</div>
            <div class="kpi-value">{value}</div>
        </div>
        """, unsafe_allow_html=True)

    kpi_card("Retard Moyen", f"{avg_delay:.2f} min", "⏱️", col1, "warning" if avg_delay > 5 else "neutral")
    kpi_card("Retard Max", f"{max_delay:.0f} min", "🛑", col2, "critical" if max_delay > 20 else "neutral")
    kpi_card("Annulations", f"{cancel_rate:.1f}%", "🚫", col3, "critical" if cancel_rate > 5 else "neutral")
    kpi_card("Impact Pluie", f"{weather_impact:.2f} min", "🌧️", col4)

    # PREDICTIONS SECTION
    st.markdown("---")
    st.header("🔮 Prédictions de Retard (LSTM)")
    
    # Load Predictions
    @st.cache_data(ttl=60)
    def load_predictions():
        engine = get_engine()
        query = """
        WITH latest_status AS (
            SELECT DISTINCT ON (train_line_ride_id, eva_number)
                train_line_ride_id,
                eva_number,
                train_type,
                train_name,
                is_canceled,
                station_name
            FROM dwh.timetables_fchg_events
            ORDER BY train_line_ride_id, eva_number, id DESC
        )
        SELECT 
            p.train_line_ride_id,
            p.eva_number,
            COALESCE(s.name, ls.station_name, p.station_name, 'Inconnu') as station_name,
            ls.train_name,
            ls.train_type,
            ls.is_canceled,
            p.scheduled_time,
            p.predicted_delay_min,
            p.prediction_proba_class,
            p.predicted_at
        FROM dwh.predictions p
        LEFT JOIN dwh.v_stations s ON p.eva_number = s.eva_number
        LEFT JOIN latest_status ls ON p.train_line_ride_id = ls.train_line_ride_id AND p.eva_number = ls.eva_number
        WHERE (p.scheduled_time + (COALESCE(p.predicted_delay_min, 0) * INTERVAL '1 minute')) > NOW()
        ORDER BY p.scheduled_time ASC 
        LIMIT 100
        """
        try:
            return pd.read_sql(query, engine)
        except Exception as e:
            st.error(f"Erreur SQL Predictions: {e}")
            print(f"Erreur SQL Predictions: {e}")
            return pd.DataFrame()

    pred_df = load_predictions()
    
    if not pred_df.empty:
        # Filter predictions if station selected
        if selected_station != "Toutes":
            # Note: Station name might be missing in predictions table if not joined, 
            # but we use eva_number usually. For MVP we skip complex filtering or rely on join.
            pass

        # Display Metrics
        c1, c2, c3 = st.columns(3)
        n_critical = len(pred_df[pred_df['prediction_proba_class'] == 'CRITICAL'])
        n_possible = len(pred_df[pred_df['prediction_proba_class'] == 'POSSIBLE'])
        last_update = pred_df['predicted_at'].max().strftime('%H:%M:%S')
        
        kpi_card("Risque Critique", f"{n_critical} Trains", "⚠️", c1, "critical" if n_critical > 0 else "good")
        kpi_card("Risque Modéré", f"{n_possible} Trains", "🔸", c2, "warning" if n_possible > 0 else "good")
        kpi_card("Dernière MAJ", f"{last_update}", "🔄", c3)
        
        # Display Table with Color Highlight
        def color_risk(val):
            color = 'green'
            if val == 'CRITICAL': color = 'red'
            elif val == 'POSSIBLE': color = 'orange'
            return f'color: {color}; font-weight: bold'

        st.dataframe(
            pred_df[['station_name', 'train_name', 'train_type', 'is_canceled', 'scheduled_time', 'predicted_delay_min', 'prediction_proba_class']]
            .style.applymap(color_risk, subset=['prediction_proba_class']),
            use_container_width=True,
            column_config={
                "station_name": "Gare",
                "train_name": "Train",
                "train_type": "Type",
                "is_canceled": "Annulé ?",
                "scheduled_time": "Heure Prévue",
                "predicted_delay_min": "Retard Est. (min)",
                "prediction_proba_class": "Risque"
            }
        )
    else:
        st.info("Aucune prédiction disponible pour le moment. Le modèle doit être entraîné.")

    # Visualizations
    st.markdown("---")
    
    # Row 1: Delay Distribution & Time Series
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("Distribution des Retards")
        fig_hist = px.histogram(filtered_df, x="current_delay", nbins=50, 
                                title="Histogramme des Retards", color_discrete_sequence=['#FF4B4B'])
        st.plotly_chart(fig_hist, use_container_width=True)
    
    with c2:
        st.subheader("Évolution Temporelle")
        fig_line = px.scatter(filtered_df, x="scheduled_time", y="current_delay", 
                              color="train_type", title="Retards par Heure", opacity=0.7)
        st.plotly_chart(fig_line, use_container_width=True)

    # Row 2: Correlation Matrix (Heatmap)
    st.subheader("Corrélations Facteurs (Météo vs Retard)")
    
    corr_cols = ['current_delay', 'temperature_2m', 'precipitation', 'wind_speed_10m']
    corr_matrix = filtered_df[corr_cols].corr()
    
    fig_corr = px.imshow(corr_matrix, text_auto=True, aspect="auto", color_continuous_scale='RdBu_r',
                         title="Matrice de Corrélation")
    st.plotly_chart(fig_corr, use_container_width=True)

    # Raw Data
    with st.expander("Voir les données brutes"):
        st.dataframe(filtered_df)

if __name__ == "__main__":
    main()
