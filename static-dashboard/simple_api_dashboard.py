
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import requests
import json
from datetime import datetime

# --- CONFIG ---
st.set_page_config(
    page_title="🎧 TracktionAI Analytics Dashboard (API-Powered)", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# API Configuration
API_BASE_URL = "http://localhost:8001"  # FastAPI server URL

# --- API HELPER FUNCTIONS ---
@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_api_data(endpoint):
    """Fetch data from FastAPI endpoint with error handling"""
    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        st.error(f"❌ Cannot connect to FastAPI server at {API_BASE_URL}")
        st.info("Please ensure FastAPI is running: `uvicorn fast_api:app --reload`")
        return None
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API Error: {e}")
        return None
    except Exception as e:
        st.error(f"❌ Error fetching data from {endpoint}: {e}")
        return None

def test_api_connection():
    """Test if FastAPI server is accessible"""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False

# --- DATA LOADING FUNCTIONS ---
@st.cache_data(ttl=300)
def load_dashboard_data():
    """Load all data needed for dashboard from API endpoints"""
    
    # Test API connection first
    if not test_api_connection():
        return None
    
    data = {}
    
    # User Analytics
    data['daily_active_users'] = fetch_api_data("/user_analytics/daily_active_users")
    data['age_distribution'] = fetch_api_data("/user_analytics/age_distribution")
    data['subscription_levels'] = fetch_api_data("/engagement_analytics/by_subscription_level")
    
    # Content Analytics  
    data['genre_popularity'] = fetch_api_data("/content_analytics/genre_popularity")
    data['top_songs'] = fetch_api_data("/content_analytics/top_songs")
    data['top_artists'] = fetch_api_data("/content_analytics/top_artists")
    
    # Geographic Analytics
    data['geographic_distribution'] = fetch_api_data("/engagement_analytics/geographic_distribution")
    data['top_artist_per_state'] = fetch_api_data("/content_analytics/popular_artist_by_state")
    
    # Time Analytics
    data['hourly_patterns'] = fetch_api_data("/engagement_analytics/hourly_patterns")
    
    # Additional Analytics
    data['average_plays_per_session'] = fetch_api_data("/content_analytics/average_plays_per_session")
    
    return data

def apply_dark_theme(fig):
    """Apply consistent dark theme to plotly figures"""
    fig.update_layout(
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117',
        font_color='#fafafa',
        title_font_color='#fafafa',
        title_font_size=16,
        font_size=12,
        xaxis=dict(
            gridcolor='#404040',
            color='#fafafa',
            tickfont=dict(color='#fafafa')
        ),
        yaxis=dict(
            gridcolor='#404040',
            color='#fafafa',
            tickfont=dict(color='#fafafa')
        ),
        legend=dict(
            bgcolor='rgba(0,0,0,0)',
            font=dict(color='#fafafa')
        ),
        margin=dict(l=0, r=0, t=40, b=0)
    )
    return fig

# --- MAIN DASHBOARD ---
def main():
    st.title("🎧 TracktionAI Analytics Dashboard")
    st.markdown("**API-Powered Real-Time Dashboard** • Live data from FastAPI endpoints")
    
    # API Status
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        if test_api_connection():
            st.success("🟢 FastAPI Connected")
        else:
            st.error("🔴 FastAPI Disconnected")
            st.stop()
    
    with col2:
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
    
    with col3:
        st.markdown(f"**API:** `{API_BASE_URL}`")
    
    st.divider()
    
    # Load data
    data = load_dashboard_data()
    if not data:
        st.error("Failed to load dashboard data")
        st.stop()
    
    # --- SIDEBAR FILTERS ---
    st.sidebar.header("🎛️ Dashboard Filters")
    
    analysis_view = st.sidebar.selectbox(
        "Choose Analysis View",
        ["📊 Overview", "🎵 Content Analytics", "👥 User Demographics", 
         "🌍 Geographic Analysis", "⏰ Time Patterns"]
    )
    
    # --- MAIN CONTENT BASED ON SELECTION ---
    
    if analysis_view == "📊 Overview":
        show_overview(data)
    elif analysis_view == "🎵 Content Analytics":
        show_content_analytics(data)
    elif analysis_view == "👥 User Demographics":
        show_user_demographics(data)
    elif analysis_view == "🌍 Geographic Analysis":
        show_geographic_analysis(data)
    elif analysis_view == "⏰ Time Patterns":
        show_time_patterns(data)

def show_overview(data):
    """Display overview dashboard"""
    st.header("📊 Platform Overview")
    
    # Key Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if data['daily_active_users']:
            total_users = len(data['daily_active_users'])
            st.metric("👥 Total Users", f"{total_users:,}")
    
    with col2:
        if data['average_plays_per_session']:
            avg_plays = data['average_plays_per_session']['average_plays_per_session']
            st.metric("🎵 Avg Plays/Session", f"{avg_plays:.1f}")
    
    with col3:
        if data['subscription_levels']:
            total_plays = sum([level['play_count'] for level in data['subscription_levels']])
            st.metric("🎧 Total Plays", f"{total_plays:,}")
    
    with col4:
        if data['genre_popularity']:
            total_genres = len(data['genre_popularity'])
            st.metric("🎼 Music Genres", f"{total_genres}")
    
    st.divider()
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Genre Popularity Chart
        if data['genre_popularity']:
            df_genres = pd.DataFrame(data['genre_popularity'])
            fig_genres = px.pie(
                df_genres, 
                values='song_count', 
                names='genre_name',
                title='🎼 Music Genre Distribution'
            )
            fig_genres = apply_dark_theme(fig_genres)
            st.plotly_chart(fig_genres, use_container_width=True)
    
    with col2:
        # Subscription Levels
        if data['subscription_levels']:
            df_subs = pd.DataFrame(data['subscription_levels'])
            fig_subs = px.bar(
                df_subs,
                x='user_level',
                y='play_count',
                title='📈 Engagement by Subscription Level',
                color='play_count',
                color_continuous_scale='viridis'
            )
            fig_subs = apply_dark_theme(fig_subs)
            st.plotly_chart(fig_subs, use_container_width=True)

def show_content_analytics(data):
    """Display content analytics"""
    st.header("🎵 Content Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎤 Top Artists")
        if data['top_artists']:
            df_artists = pd.DataFrame(data['top_artists'][:10])
            fig_artists = px.bar(
                df_artists,
                x='play_count',
                y='artist_name',
                orientation='h',
                title='Most Popular Artists',
                color='play_count',
                color_continuous_scale='plasma'
            )
            fig_artists = apply_dark_theme(fig_artists)
            st.plotly_chart(fig_artists, use_container_width=True)
    
    with col2:
        st.subheader("🎵 Top Songs")
        if data['top_songs']:
            df_songs = pd.DataFrame(data['top_songs'][:10])
            # Create combined labels for better display
            df_songs['song_artist'] = df_songs['song_title'] + ' - ' + df_songs['artist_name']
            fig_songs = px.bar(
                df_songs,
                x='play_count',
                y='song_artist',
                orientation='h',
                title='Most Popular Songs',
                color='play_count',
                color_continuous_scale='cividis'
            )
            fig_songs = apply_dark_theme(fig_songs)
            st.plotly_chart(fig_songs, use_container_width=True)

def show_user_demographics(data):
    """Display user demographics"""
    st.header("👥 User Demographics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Age Distribution
        if data['age_distribution']:
            df_age = pd.DataFrame(data['age_distribution'])
            fig_age = px.bar(
                df_age,
                x='age_group',
                y='user_count',
                title='👥 User Age Distribution',
                color='user_count',
                color_continuous_scale='blues'
            )
            fig_age = apply_dark_theme(fig_age)
            st.plotly_chart(fig_age, use_container_width=True)
    
    with col2:
        # Subscription Analysis
        if data['subscription_levels']:
            df_subs = pd.DataFrame(data['subscription_levels'])
            fig_pie = px.pie(
                df_subs,
                values='play_count',
                names='user_level',
                title='💰 User Subscription Breakdown'
            )
            fig_pie = apply_dark_theme(fig_pie)
            st.plotly_chart(fig_pie, use_container_width=True)

def show_geographic_analysis(data):
    """Display geographic analytics"""
    st.header("🌍 Geographic Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🗺️ Geographic Distribution")
        if data['geographic_distribution']:
            df_geo = pd.DataFrame(data['geographic_distribution'])
            # Group by state and sum play counts
            state_totals = df_geo.groupby('state')['play_count'].sum().reset_index()
            state_totals = state_totals.sort_values('play_count', ascending=False).head(15)
            
            fig_geo = px.bar(
                state_totals,
                x='state',
                y='play_count',
                title='Top States by Play Count',
                color='play_count',
                color_continuous_scale='viridis'
            )
            fig_geo.update_xaxes(tickangle=45)
            fig_geo = apply_dark_theme(fig_geo)
            st.plotly_chart(fig_geo, use_container_width=True)
    
    with col2:
        st.subheader("🎤 Top Artist by State")
        if data['top_artist_per_state']:
            # Convert dict to list for display
            state_artists = []
            for state, info in data['top_artist_per_state'].items():
                state_artists.append({
                    'state': state,
                    'artist': info['artist'],
                    'play_count': info['play_count']
                })
            
            df_state_artists = pd.DataFrame(state_artists[:10])
            
            for _, row in df_state_artists.iterrows():
                st.markdown(f"**{row['state']}**: {row['artist']} ({row['play_count']} plays)")

def show_time_patterns(data):
    """Display time-based analytics"""
    st.header("⏰ Time Patterns")
    
    if data['hourly_patterns']:
        df_hourly = pd.DataFrame(data['hourly_patterns'])
        
        # Hourly listening patterns
        fig_hourly = px.line(
            df_hourly,
            x='hour',
            y='play_count',
            title='🕒 Listening Activity by Hour of Day',
            markers=True
        )
        fig_hourly.update_xaxes(title='Hour of Day (0-23)')
        fig_hourly.update_yaxes(title='Play Count')
        fig_hourly = apply_dark_theme(fig_hourly)
        st.plotly_chart(fig_hourly, use_container_width=True)
        
        # Peak hours analysis
        peak_hour = df_hourly.loc[df_hourly['play_count'].idxmax()]
        st.info(f"🎯 Peak listening hour: **{peak_hour['hour']}:00** with **{peak_hour['play_count']}** plays")

if __name__ == "__main__":
    main()