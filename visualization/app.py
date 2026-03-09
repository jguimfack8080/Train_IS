import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
import math

# Timezone Configuration
BERLIN_TZ = ZoneInfo("Europe/Berlin")

# Page config
st.set_page_config(
    page_title="Train_IS - Verspätungsprognose-Dashboard",
    page_icon="🚄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize Session State
if 'active_filter' not in st.session_state:
    st.session_state.active_filter = 'all'  # Options: 'all', 'canceled', 'high_risk', 'delayed'
if 'page_number' not in st.session_state:
    st.session_state.page_number = 1
if 'rows_per_page' not in st.session_state:
    st.session_state.rows_per_page = 20

# Database Connection
@st.cache_resource
def get_db_engine():
    # Let's stick to standard env vars from docker-compose
    user = os.getenv("DATA_DB_USER", "dw")
    password = os.getenv("DATA_DB_PASSWORD", "dw")
    host = os.getenv("DATA_DB_HOST", "postgres")
    port = os.getenv("DATA_DB_PORT", "5432")
    dbname = os.getenv("DATA_DB_NAME", "train_dw")
    
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")

@st.cache_data(ttl=60, show_spinner=False)
def load_data(historical=False):
    engine = get_db_engine()
    
    if historical:
        # Load last 7 days for historical analysis
        time_filter = "WHERE p.scheduled_time >= NOW() - INTERVAL '7 DAYS'"
        limit = ""
    else:
        # Load NOW to NOW + 24h (Real-time window)
        # User requested starting exactly from current time
        time_filter = "WHERE p.scheduled_time >= NOW() AND p.scheduled_time <= NOW() + INTERVAL '24 HOURS'"
        limit = ""

    query = f"""
    WITH predictions_data AS (
        SELECT 
            p.train_line_ride_id,
            p.station_name,
            p.scheduled_time,
            p.predicted_delay_min,
            p.prediction_proba_class,
            p.confidence_score,
            p.predicted_at
        FROM dwh.predictions p
        {time_filter}
        ORDER BY p.scheduled_time ASC
        {limit}
    ),
    train_info AS (
        SELECT DISTINCT ON (train_line_ride_id)
            train_line_ride_id,
            train_number,
            train_line_name as train_line,
            train_category as train_type,
            train_direction,
            route_path,
            platform as scheduled_platform,
            -- Pre-calculate parsed direction from route_path (last element)
            split_part(route_path, '|', array_length(string_to_array(route_path, '|'), 1)) as parsed_direction
        FROM dwh.timetables_plan_events
    ),
    train_status AS (
        SELECT DISTINCT ON (train_line_ride_id)
            train_line_ride_id,
            is_canceled,
            train_name,
            delay_in_min
        FROM dwh.timetables_fchg_events
        ORDER BY train_line_ride_id, event_time DESC
    ),
    train_changes AS (
        SELECT DISTINCT ON (event_id)
            event_id as train_line_ride_id,
            platform as changed_platform,
            platform_change
        FROM dwh.timetables_rchg_events
        ORDER BY event_id, timestamp_event DESC
    )
    SELECT 
        pd.*,
        COALESCE(ts.train_name, ti.train_number, split_part(pd.train_line_ride_id, '-', 2)) as train_number,
        ti.train_line,
        ti.train_type,
        -- Use the last station in route_path as true direction/destination if available
        -- Filter out numeric/short garbage (e.g. "5", "7", "8Süd") by checking if it starts with a digit
        COALESCE(
            CASE WHEN ti.parsed_direction !~ '^[0-9]' THEN ti.parsed_direction END,
            CASE WHEN ti.train_direction !~ '^[0-9]' THEN ti.train_direction END,
            'Unbekanntes Ziel'
        ) as train_direction,
        ti.route_path,
        ti.scheduled_platform,
        tc.changed_platform,
        COALESCE(tc.changed_platform, ti.scheduled_platform) as current_platform,
        CASE 
            WHEN tc.changed_platform IS NOT NULL AND tc.changed_platform != ti.scheduled_platform THEN TRUE 
            ELSE FALSE 
        END as is_platform_changed,
        COALESCE(ts.is_canceled, FALSE) as is_canceled,
        ts.delay_in_min as current_delay
    FROM predictions_data pd
    LEFT JOIN train_info ti ON pd.train_line_ride_id = ti.train_line_ride_id
    LEFT JOIN train_status ts ON pd.train_line_ride_id = ts.train_line_ride_id
    LEFT JOIN train_changes tc ON pd.train_line_ride_id = tc.train_line_ride_id
    """
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        
        # Convert timestamps to Berlin Timezone
        if not df.empty:
            for col in ['scheduled_time', 'predicted_at']:
                if col in df.columns:
                    # Ensure datetime is timezone-aware (assume UTC if naive)
                    if df[col].dt.tz is None:
                         df[col] = df[col].dt.tz_localize(timezone.utc)
                    
                    # Convert to Berlin Time
                    df[col] = df[col].dt.tz_convert(BERLIN_TZ)
        
        return df
    except Exception as e:
        st.error(f"Fehler beim Laden der Daten: {e}")
        return pd.DataFrame()

# Timeline Generation Function (HTML)
def render_timeline(route_path):
    if not route_path or pd.isna(route_path):
        return '<div style="color: #666; font-style: italic; padding: 10px;">Unbekannte Strecke</div>'
    
    # Parsing: Split by |, trim spaces
    stops = [s.strip() for s in str(route_path).split('|') if s.strip()]
    
    if not stops:
        return '<div style="color: #666; font-style: italic; padding: 10px;">Unbekannte Strecke</div>'

    # Using horizontal scroll for "All visible" requirement
    container_style = "display: flex; align-items: flex-start; overflow-x: auto; padding: 15px 5px; scrollbar-width: thin; width: 100%;"
    
    html_parts = []
    html_parts.append(f'<div style="{container_style}">')
    
    for i, stop in enumerate(stops):
        is_first = (i == 0)
        is_last = (i == len(stops) - 1)
        
        # Connector Line (draw before the node, unless it's the first node)
        if i > 0:
            html_parts.append('<div style="min-width: 40px; height: 2px; background-color: #ccc; margin-top: 7px; flex-shrink: 0;"></div>')
            
        # Node Style
        if is_first:
            # Start: Distinct marker (Green filled circle) + Bold Text
            marker_style = "width: 16px; height: 16px; background-color: #2e7d32; border-radius: 50%; border: 2px solid #fff; box-shadow: 0 0 0 2px #2e7d32;"
            text_style = "font-weight: bold; color: #2e7d32; font-size: 0.9em;"
        elif is_last:
            # End: Distinct marker (Red filled circle) + Bold Text
            marker_style = "width: 16px; height: 16px; background-color: #d32f2f; border-radius: 50%; border: 2px solid #fff; box-shadow: 0 0 0 2px #d32f2f;"
            text_style = "font-weight: bold; color: #d32f2f; font-size: 0.9em;"
        else:
            # Intermediate: Smaller dot + Normal Text
            marker_style = "width: 10px; height: 10px; background-color: #666; border-radius: 50%; margin-top: 3px;"
            text_style = "font-weight: normal; color: #444; font-size: 0.8em;"
            
        # Node Container
        # min-width ensures text doesn't squash too much, but scroll handles the rest
        node_html = f"""
        <div style="display: flex; flex-direction: column; align-items: center; min-width: 100px; flex-shrink: 0; position: relative;">
            <div style="{marker_style} margin-bottom: 8px;"></div>
            <div style="{text_style} text-align: center; white-space: normal; line-height: 1.2; word-wrap: break-word; max-width: 120px;" title="{stop}">
                {stop}
            </div>
        </div>
        """
        html_parts.append(node_html)
        
    html_parts.append('</div>')
    return "".join(html_parts)

# Dialog for Train Details
@st.dialog("Fahrtdetails", width="large")
def show_train_details(row):
    # Header Info
    c1, c2 = st.columns([2, 1])
    with c1:
        st.subheader(f"🚆 {row['train_type']} {row['train_number']}")
        st.caption(f"Nach {row['train_direction']}")
        
        # Platform Info
        platform = row.get('current_platform')
        if pd.isna(platform):
            platform = "Unbekannt"
        
        if row.get('is_platform_changed', False):
            st.warning(f"⚠️ Gleisänderung: Geplant {row.get('scheduled_platform', '?')} → Aktuell {platform}")
        else:
            st.markdown(f"**Gleis:** {platform}")
            
    with c2:
        delay = row['predicted_delay_min']
        if pd.notna(delay):
            color = "red" if delay > 5 else ("orange" if delay > 2 else "green")
            st.markdown(f"**Verspätung:** :{color}[{delay:.1f} Min.]")
        else:
            st.markdown("**Verspätung:** N/A")
            
    st.divider()
    
    # Timeline
    st.markdown("### 📍 Vollständige Route")
    st.markdown(render_timeline(row['route_path']), unsafe_allow_html=True)
    
    st.divider()
    
    # Additional Details
    cols = st.columns(3)
    cols[0].metric("Aktueller Bahnhof", row['station_name'])
    cols[1].metric("Geplante Zeit", row['scheduled_time'].strftime('%H:%M'))
    cols[2].metric("Wahrscheinlichkeit", row.get('risk_display', 'N/A'))


# Sidebar
st.sidebar.title("🚄 Train_IS")
st.sidebar.markdown("---")

# Toggle for Historical Data
show_historical = st.sidebar.checkbox("📜 Vollständigen Verlauf anzeigen", value=False, help="Aktivieren, um alle vergangenen Vorhersagen zu sehen.")

# Refresh Button
if st.sidebar.button("🔄 Daten aktualisieren"):
    st.cache_data.clear()

# Main Content
now = datetime.now(timezone.utc).astimezone(BERLIN_TZ)
st.title("Prognose-Dashboard")

# Header Info
st.markdown(f"""
<div style="display: flex; justify-content: space-between; align_items: center; margin-bottom: 20px; padding: 15px; background-color: #f0f2f6; border-radius: 10px; border-left: 5px solid #ff4b4b;">
    <div>
        <h3 style="margin:0; color: #0e1117;">Systemstatus</h3>
        <p style="margin:0; color: #555;">Letzte Aktualisierung: <strong>{now.strftime('%d.%m.%Y um %H:%M')}</strong></p>
    </div>
    <div style="text-align: right;">
        <p style="margin:0; font-size: 1.1em;">Modus: <strong>{'Verlauf' if show_historical else 'Echtzeit (Zukunft 24h)'}</strong></p>
    </div>
</div>
""", unsafe_allow_html=True)

# Load Data
df = load_data(historical=show_historical)

if df.empty:
    st.warning("⚠️ Keine Vorhersagen für den gewählten Zeitraum verfügbar.")
    st.info("Das System generiert automatisch Vorhersagen. Wenn keine Daten erscheinen, überprüfen Sie, ob die Vorhersage-Pipeline aktiv ist.")
else:
    # Preprocessing for visualization
    df['is_canceled'] = df['is_canceled'].fillna(False).astype(bool)
    
    # Check for platform changes and notify
    if 'is_platform_changed' in df.columns:
        df['is_platform_changed'] = df['is_platform_changed'].fillna(False).astype(bool)
        changed_trains = df[df['is_platform_changed']]
        if not changed_trains.empty:
            count = len(changed_trains)
            msg = f"⚠️ Gleisänderung für {count} Zug/Züge erkannt!"
            st.toast(msg, icon="📢")
    
    # Translate Risk Levels
    risk_mapping = {"High": "Hoch", "Medium": "Mittel", "Low": "Niedrig"}
    if 'prediction_proba_class' in df.columns:
        df['risk_display'] = df['prediction_proba_class'].map(risk_mapping).fillna(df['prediction_proba_class'])
    else:
        df['risk_display'] = "Unbekannt"
    
    # Key Metrics as Filters
    # Calculate counts first
    total_trains = len(df['train_line_ride_id'].unique())
    cancelled_trains = len(df[df['is_canceled']]['train_line_ride_id'].unique())
    
    # Calculate delay metrics
    df_active = df[~df['is_canceled']]
    avg_delay = df_active['predicted_delay_min'].mean() if not df_active.empty else 0.0
    
    high_risk_count = len(df[(df['predicted_delay_min'] > 5) & (~df['is_canceled'])])
    
    # Next train countdown
    next_train_time = df[df['scheduled_time'] > now]['scheduled_time'].min()
    if pd.notnull(next_train_time):
        delta = next_train_time - now
        minutes = int(delta.total_seconds() / 60)
        next_train_label = f"In {minutes} Min."
    else:
        next_train_label = "N/A"

    # Define columns for interactive cards
    col1, col2, col3, col4 = st.columns(4)

    # Helper style for active state
    def get_button_type(filter_name):
        return "primary" if st.session_state.active_filter == filter_name else "secondary"

    with col1:
        # Filter: All (Future)
        if st.button(
            f"🚆 Nächste Züge\n{total_trains}", 
            key="btn_all", 
            type=get_button_type('all'), 
            use_container_width=True,
            help="Alle geplanten Züge anzeigen (Filter zurücksetzen)"
        ):
            st.session_state.active_filter = 'all'
            st.session_state.page_number = 1 # Reset page
            st.rerun()

    with col2:
        # Filter: High Risk
        if st.button(
            f"⚠️ Hohes Risiko (>5min)\n{high_risk_count}", 
            key="btn_high_risk", 
            type=get_button_type('high_risk'), 
            use_container_width=True,
            help="Nur Züge mit einer prognostizierten Verspätung > 5 Min. anzeigen"
        ):
            st.session_state.active_filter = 'high_risk'
            st.session_state.page_number = 1 # Reset page
            st.rerun()

    with col3:
        # Filter: Canceled
        if st.button(
            f"❌ Ausgefallen\n{cancelled_trains}", 
            key="btn_canceled", 
            type=get_button_type('canceled'), 
            use_container_width=True,
            help="Nur ausgefallene Züge anzeigen"
        ):
            st.session_state.active_filter = 'canceled'
            st.session_state.page_number = 1 # Reset page
            st.rerun()

    with col4:
        # Informational Metric (Next Train Countdown) - clicking resets to 'all' or just refreshes
        if st.button(
            f"⏱️ Nächster Zug\n{next_train_label}", 
            key="btn_next", 
            type="secondary", 
            use_container_width=True,
            help="Zeit bis zur nächsten Abfahrt (Klicken, um alle Züge zu sehen)"
        ):
            st.session_state.active_filter = 'all'
            st.session_state.page_number = 1 # Reset page
            st.rerun()

    # Show active filter message
    if st.session_state.active_filter != 'all':
        filter_labels = {
            'high_risk': "⚠️ Hochrisiko-Züge (> 5 Min.)",
            'canceled': "❌ Ausgefallene Züge",
            'delayed': "🐢 Verspätete Züge"
        }
        st.info(f"Aktiver Filter: **{filter_labels.get(st.session_state.active_filter, 'Benutzerdefiniert')}** (Klicken Sie auf 'Nächste Züge', um alle zu sehen)")

    # Filters
    st.markdown("### 🔍 Erweiterte Filter")
    c1, c2, c3 = st.columns(3)
    with c1:
        stations = ["Alle"] + list(df['station_name'].unique())
        selected_station = st.selectbox("Bahnhof", stations)
    with c2:
        types = ["Alle"] + list(df['train_type'].unique())
        selected_type = st.selectbox("Zugtyp", types)
    with c3:
        # Pagination Settings
        rows_selection = st.selectbox("Zeilen pro Seite", [20, 50, 100, "Alle"], index=0)
        
        if rows_selection == "Alle":
             # Use a large number to effectively show all rows
             st.session_state.rows_per_page = 1000000 
        else:
             st.session_state.rows_per_page = rows_selection

    # Apply Filters
    df_filtered = df.copy()
    
    # 1. Apply Session State Filters (Button Clicks)
    if st.session_state.active_filter == 'canceled':
        df_filtered = df_filtered[df_filtered['is_canceled']]
    elif st.session_state.active_filter == 'high_risk':
        df_filtered = df_filtered[(df_filtered['predicted_delay_min'] > 5) & (~df_filtered['is_canceled'])]
    
    # 2. Apply Dropdown Filters (Refined logic)
    if selected_station != "Alle":
        df_filtered = df_filtered[df_filtered['station_name'] == selected_station]
    if selected_type != "Alle":
        df_filtered = df_filtered[df_filtered['train_type'] == selected_type]

    # DataFrame with Selection - MOVED UP for immediate visibility
    st.markdown("### 📋 Liste der gefilterten Züge")
    st.caption("💡 Klicken Sie auf eine Zeile, um Details zur Fahrt zu sehen.")
    
    # Sort by time
    df_sorted = df_filtered.sort_values("scheduled_time").reset_index(drop=True)
    
    # --- Pagination Logic ---
    total_rows = len(df_sorted)
    total_pages = math.ceil(total_rows / st.session_state.rows_per_page)
    
    # Ensure page number is valid
    if st.session_state.page_number > total_pages:
        st.session_state.page_number = max(1, total_pages)
        
    start_idx = (st.session_state.page_number - 1) * st.session_state.rows_per_page
    end_idx = start_idx + st.session_state.rows_per_page
    
    # Slice Dataframe for display
    df_page = df_sorted.iloc[start_idx:end_idx].copy()
    
    # Create readable route string for table view (replace | with arrow)
    if "route_path" in df_page.columns:
        df_page["route_display"] = df_page["route_path"].astype(str).str.replace("|", " → ")
    else:
        df_page["route_display"] = ""

    # Add Status Column for better visibility
    def get_status(row):
        if row['is_canceled']:
            return "❌ Ausgefallen"
        delay = row['predicted_delay_min']
        if pd.isna(delay):
            return "❓ Unbekannt"
        if delay > 5:
            return "⚠️ Hohes Risiko"
        elif delay > 2:
            return "🐢 Verspätung"
        else:
            return "✅ Pünktlich"
            
    df_page['status_display'] = df_page.apply(get_status, axis=1)

    # Prepare DataFrame for display (select columns)
    df_display = df_page[[
        "scheduled_time", "train_number", "train_type", "station_name", "train_direction", "current_platform",
        "status_display", "predicted_delay_min", "risk_display", 
        "route_display"
    ]]
    
    # Display Pagination Controls
    col_p1, col_p2, col_p3, col_p4 = st.columns([1, 1, 3, 1])
    with col_p1:
        if st.button("⬅️ Zurück", disabled=(st.session_state.page_number <= 1)):
            st.session_state.page_number -= 1
            st.rerun()
    with col_p2:
        if st.button("Weiter ➡️", disabled=(st.session_state.page_number >= total_pages)):
            st.session_state.page_number += 1
            st.rerun()
    with col_p3:
        st.markdown(f"**Seite {st.session_state.page_number} von {total_pages}** ({total_rows} Einträge)")
    
    event = st.dataframe(
        df_display,
        column_config={
            "scheduled_time": st.column_config.DatetimeColumn("Zeit", format="HH:mm"),
            "train_number": "Zug-Nr.",
            "train_type": "Typ",
            "station_name": "Bahnhof",
            "train_direction": "Ziel",
            "current_platform": st.column_config.TextColumn("Gleis"),
            "status_display": "Status",
            "predicted_delay_min": st.column_config.NumberColumn("Verspätung (Min.)", format="%.1f"),
            "risk_display": "Risiko",
            "route_display": st.column_config.TextColumn("Strecke", help="Vollständige Route", width="large"),
        },
        use_container_width=True,
        hide_index=True,
        selection_mode="single-row",
        on_select="rerun"
    )
    
    # Handle Selection
    if len(event.selection.rows) > 0:
        selected_index = event.selection.rows[0]
        # Map selection index back to original dataframe (or page dataframe)
        selected_row = df_page.iloc[selected_index]
        show_train_details(selected_row)
        
    st.divider()

    # Visualizations - Shown only if relevant (not solely canceled view)
    if st.session_state.active_filter != 'canceled':
        st.markdown("### 📊 Risikoanalyse")
        
        if df_filtered.empty:
            st.info("Keine Daten entsprechen den ausgewählten Filtern.")
        else:
            # Separate cancelled trains for visualization logic
            df_active = df_filtered[~df_filtered['is_canceled']].copy()
            
            # Scatter Plot: Delay vs Time
            if not df_active.empty:
                # Create size reference first to handle negative values
                df_active['size_ref'] = df_active['predicted_delay_min'].abs().clip(lower=1)
                
                fig_scatter = px.scatter(
                    df_active,
                    x="scheduled_time",
                    y="predicted_delay_min",
                    size="size_ref", 
                    color="risk_display",
                    hover_data=["train_number", "station_name", "train_direction", "predicted_delay_min"],
                    title="Verspätungsprognosen (Aktive Züge)",
                    labels={"scheduled_time": "Geplante Zeit", "predicted_delay_min": "Geschätzte Verspätung (Min.)", "risk_display": "Risiko"},
                    color_discrete_map={"Hoch": "red", "Mittel": "orange", "Niedrig": "green"}
                )
                # Update size reference scaling
                max_size = df_active['size_ref'].max() if not df_active.empty else 1
                fig_scatter.update_traces(marker=dict(sizemode='area', sizeref=2.*max_size/(40.**2), sizemin=4))
                
                st.plotly_chart(fig_scatter, use_container_width=True)

                # --- Additional Visualizations (Heatmap & Pie Chart) ---
                st.markdown("### 📈 Detailanalysen")
                
                # Pie Chart: Risk Distribution (Full Width or smaller, but separate from Heatmap)
                risk_counts = df_active['risk_display'].value_counts().reset_index()
                risk_counts.columns = ['risk_display', 'count']
                
                # Enhanced Pie Chart (Donut style)
                fig_pie = px.pie(
                    risk_counts, 
                    values='count', 
                    names='risk_display', 
                    title='Verteilung der Risikoklassen',
                    color='risk_display',
                    color_discrete_map={"Hoch": "#DC3545", "Mittel": "#FFC107", "Niedrig": "#28A745"},
                    hole=0.4
                )
                fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                fig_pie.update_layout(showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5))
                st.plotly_chart(fig_pie, use_container_width=True)
                
                st.divider()

                # Heatmap: Delay by Hour and Station (Full Width at the bottom)
                # Extract hour from scheduled_time if not already present
                df_active['hour'] = df_active['scheduled_time'].dt.hour
                
                # Pivot table for Heatmap
                heatmap_data = df_active.pivot_table(
                    index='station_name', 
                    columns='hour', 
                    values='predicted_delay_min', 
                    aggfunc='mean'
                ).fillna(0)
                
                if not heatmap_data.empty:
                    # Enhanced Heatmap
                    fig_heatmap = px.imshow(
                        heatmap_data,
                        labels=dict(x="Uhrzeit (Stunde)", y="Station", color="Verspätung (Min)"),
                        x=heatmap_data.columns,
                        y=heatmap_data.index,
                        title="Heatmap: Verspätung nach Station & Zeit",
                        color_continuous_scale="RdYlGn_r",
                        aspect="auto",
                        text_auto=".1f"
                    )
                    fig_heatmap.update_xaxes(side="bottom")
                    # Make the heatmap taller to be more visible as requested
                    fig_heatmap.update_layout(height=600)
                    st.plotly_chart(fig_heatmap, use_container_width=True)
                else:
                    st.info("Nicht genügend Daten für die Heatmap.")

            else:
                st.info("Keine aktiven (nicht stornierten) Züge für die Visualisierung.")
