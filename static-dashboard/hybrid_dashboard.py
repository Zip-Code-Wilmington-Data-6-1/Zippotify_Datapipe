import pandas as pd
import plotly.express as px
import streamlit as st
import requests
import json
from datetime import datetime

# --- CONFIG ---
st.set_page_config(
    page_title="🎧 TracktionAI Hybrid Dashboard", 
    layout="wide"
)

API_BASE_URL = "http://localhost:8000"

# --- HYBRID DATA LOADING ---
@st.cache_data(ttl=300)
def load_hybrid_data():
    """Load data from both API and CSV sources"""
    data = {'source': 'hybrid'}
    
    # Try API first
    try:
        api_available = requests.get(f"{API_BASE_URL}/health", timeout=3).status_code == 200
    except:
        api_available = False
    
    if api_available:
        st.sidebar.success("🟢 Using Live API Data")
        # Load from API
        data['daily_active_users'] = requests.get(f"{API_BASE_URL}/user_analytics/daily_active_users").json()
        data['genre_popularity'] = requests.get(f"{API_BASE_URL}/content_analytics/genre_popularity").json()
        data['top_artists'] = requests.get(f"{API_BASE_URL}/content_analytics/top_artists").json()
        data['source'] = 'api'
    else:
        st.sidebar.warning("🟡 Using Static CSV Data")
        # Fallback to CSV
        try:
            data['daily_active_users'] = pd.read_csv('aggregated_data/daily_active_users.csv').to_dict('records')
            data['genre_popularity'] = pd.read_csv('aggregated_data/genre_popularity.csv').to_dict('records')
            data['top_artists'] = pd.read_csv('aggregated_data/top_artists.csv').to_dict('records')
            data['source'] = 'csv'
        except FileNotFoundError:
            st.error("❌ Neither API nor CSV data available")
            return None
    
    return data

def main():
    st.title("🎧 TracktionAI Hybrid Dashboard")
    st.markdown("**Smart Data Loading** • API when available, CSV as fallback")
    
    data = load_hybrid_data()
    if not data:
        st.stop()
    
    # Show data source
    source_emoji = "🚀" if data['source'] == 'api' else "📊"
    st.info(f"{source_emoji} Data Source: {'Live API' if data['source'] == 'api' else 'Static CSV Files'}")
    
    # Your existing dashboard logic here...
    if data['genre_popularity']:
        df_genres = pd.DataFrame(data['genre_popularity'])
        fig = px.pie(df_genres, values='song_count', names='genre_name', title='Genre Distribution')
        st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()