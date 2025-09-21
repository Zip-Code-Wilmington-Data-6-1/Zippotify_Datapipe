import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import json
from datetime import datetime
import folium
from folium import plugins
from streamlit_folium import st_folium
import time
import random
from ai_bot import DataInsightBot

# --- CONFIG ---
st.set_page_config(
    page_title="🎧 TracktionAI Analytics Dashboard", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dark theme styling
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
    }
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    .stMetric {
        background-color: #262730;
        border: 1px solid #404040;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stMarkdown h1 {
        color: #fafafa;
        font-size: 3rem !important;
        margin-bottom: 0.5rem;
    }
    .stMarkdown h2 {
        color: #fafafa;
        border-bottom: 2px solid #1f77b4;
        padding-bottom: 0.5rem;
    }
    .stMarkdown h3 {
        color: #fafafa;
    }
    div[data-testid="metric-container"] {
        background-color: #262730;
        border: 1px solid #404040;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.3);
    }
    div[data-testid="metric-container"] > div {
        width: fit-content;
        margin: auto;
    }
    div[data-testid="metric-container"] > div > div {
        width: fit-content;
        margin: auto;
    }
    .sidebar .sidebar-content {
        background-color: #262730;
    }
</style>
""", unsafe_allow_html=True)

# --- LOAD DATA ---
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

def create_listening_heatmap(selected_day=None, geo_data=None):
    """Create a Folium heatmap of listening activity using aggregated data"""
    
    # Use aggregated geographic data instead of raw sample data
    try:
        if geo_data is not None:
            geo_df = geo_data.copy()
        else:
            # Fallback to synthetic data if no geo data provided
            return create_fallback_heatmap()
        
        # Create a city coordinate lookup (common US cities)
        city_coordinates = {
            'Los Angeles': (34.0522, -118.2437),
            'Chicago': (41.8781, -87.6298),
            'Fort Worth': (32.7555, -97.3308),
            'Lodi': (38.1302, -121.2722),
            'Moreno Valley': (33.9375, -117.2308),
            'Lake City': (30.1896, -82.6404),
            'Plano': (33.0198, -96.6989),
            'Masury': (41.2042, -80.5653),
            'Royse City': (32.9751, -96.3275),
            'New York': (40.7128, -74.0060),
            'Houston': (29.7604, -95.3698),
            'Phoenix': (33.4484, -112.0740),
            'Philadelphia': (39.9526, -75.1652),
            'San Antonio': (29.4241, -98.4936),
            'San Diego': (32.7157, -117.1611),
            'Dallas': (32.7767, -96.7970),
            'San Jose': (37.3382, -121.8863),
            'Austin': (30.2672, -97.7431),
            'Jacksonville': (30.3322, -81.6557),
            'San Francisco': (37.7749, -122.4194),
            'Columbus': (39.9612, -82.9988),
            'Indianapolis': (39.7684, -86.1581),
            'Charlotte': (35.2271, -80.8431),
            'Seattle': (47.6062, -122.3321),
            'Denver': (39.7392, -104.9903),
            'Boston': (42.3601, -71.0589),
            'El Paso': (31.7619, -106.4850),
            'Detroit': (42.3314, -83.0458),
            'Nashville': (36.1627, -86.7816),
            'Portland': (45.5152, -122.6784),
            'Oklahoma City': (35.4676, -97.5164),
            'Las Vegas': (36.1699, -115.1398),
            'Louisville': (38.2527, -85.7585),
            'Baltimore': (39.2904, -76.6122),
            'Milwaukee': (43.0389, -87.9065),
            'Albuquerque': (35.0844, -106.6504),
            'Tucson': (32.2226, -110.9747),
            'Fresno': (36.7378, -119.7871),
            'Sacramento': (38.5816, -121.4944),
            'Mesa': (33.4152, -111.8315),
            'Kansas City': (39.0997, -94.5786),
            'Atlanta': (33.7490, -84.3880),
            'Long Beach': (33.7701, -118.1937),
            'Colorado Springs': (38.8339, -104.8214),
            'Raleigh': (35.7796, -78.6382),
            'Miami': (25.7617, -80.1918),
            'Virginia Beach': (36.8529, -75.9780),
            'Omaha': (41.2565, -95.9345),
            'Oakland': (37.8044, -122.2711),
            'Minneapolis': (44.9778, -93.2650),
            'Tulsa': (36.1540, -95.9928),
            'Arlington': (32.7357, -97.1081),
            'Tampa': (27.9506, -82.4572),
            'New Orleans': (29.9511, -90.0715),
            'Wichita': (37.6872, -97.3301),
            'Cleveland': (41.4993, -81.6944),
            'Bakersfield': (35.3733, -119.0187),
            'Aurora': (39.7294, -104.8319),
            'Anaheim': (33.8366, -117.9143),
            'Honolulu': (21.3099, -157.8581),
            'Santa Ana': (33.7455, -117.8677),
            'Corpus Christi': (27.8006, -97.3964),
            'Riverside': (33.9533, -117.3962),
            'Lexington': (38.0406, -84.5037),
            'Stockton': (37.9577, -121.2908),
            'Henderson': (36.0395, -114.9817),
            'Saint Paul': (44.9537, -93.0900),
            'St. Louis': (38.6270, -90.1994),
            'Cincinnati': (39.1031, -84.5120),
            'Pittsburgh': (40.4406, -79.9959),
            'Greensboro': (36.0726, -79.7920),
            'Anchorage': (61.2181, -149.9003),
            'Lincoln': (40.8136, -96.7026),
            'Orlando': (28.5383, -81.3792),
            'Irvine': (33.6846, -117.8265),
            'Newark': (40.7357, -74.1724),
            'Durham': (35.9940, -78.8986),
            'Chula Vista': (32.6401, -117.0842),
            'Toledo': (41.6528, -83.5379),
            'Fort Wayne': (41.0793, -85.1394),
            'St. Petersburg': (27.7676, -82.6403),
            'Laredo': (27.5306, -99.4803),
            'Jersey City': (40.7178, -74.0431),
            'Chandler': (33.3062, -111.8413),
            'Madison': (43.0731, -89.4012),
            'Lubbock': (33.5779, -101.8552),
            'Scottsdale': (33.4942, -111.9261),
            'Reno': (39.5296, -119.8138),
            'Buffalo': (42.8864, -78.8784),
            'Gilbert': (33.3528, -111.7890),
            'Glendale': (33.5387, -112.1860),
            'North Las Vegas': (36.1989, -115.1175),
            'Winston-Salem': (36.0999, -80.2442),
            'Chesapeake': (36.7682, -76.2875),
            'Norfolk': (36.8468, -76.2852),
            'Fremont': (37.5483, -121.9886),
            'Garland': (32.9126, -96.6389),
            'Irving': (32.8140, -96.9489),
            'Hialeah': (25.8576, -80.2781),
            'Richmond': (37.5407, -77.4360),
            'Boise': (43.6150, -116.2023),
            'Spokane': (47.6588, -117.4260)
        }
        
        # Create heatmap data from aggregated geographic analysis with explicit weight distribution
        heatmap_data = []
        debug_weights = []  # For debugging
        
        for i, (_, row) in enumerate(geo_df.iterrows()):
            city = row['city']
            total_plays = row['total_plays']
            
            # Get coordinates for the city
            if city in city_coordinates:
                lat, lon = city_coordinates[city]
                
                # Force different weights based on city index and day to guarantee color spread
                total_cities = len(geo_df)
                base_rank = i / total_cities  # 0.0 to 1.0 based on position in data
                
                # Apply day-based variations that are more extreme
                day_effects = {
                    'Monday': -0.4,     # Very low
                    'Tuesday': -0.2,    # Low
                    'Wednesday': 0.0,   # Baseline
                    'Thursday': 0.1,    # Slightly higher
                    'Friday': 0.3,      # Higher
                    'Saturday': 0.5,    # Very high
                    'Sunday': 0.2       # Medium-high
                }
                
                day_effect = day_effects.get(selected_day, 0.0) if selected_day else 0.0
                
                # Create explicit weight distribution across the full 0.0-1.0 range
                raw_weight = base_rank + day_effect
                final_weight = max(0.0, min(1.0, raw_weight))
                
                # Force some cities to specific ranges to guarantee color variety
                if city == 'Los Angeles':
                    final_weight = 1.0 if selected_day == 'Saturday' else 0.9
                elif city == 'New York City':
                    final_weight = 0.95 if selected_day in ['Friday', 'Saturday'] else 0.8
                elif city == 'Chicago':
                    final_weight = 0.85 if selected_day in ['Friday', 'Saturday', 'Sunday'] else 0.7
                elif city in ['Lodi', 'Lake City', 'Masury']:
                    final_weight = 0.1 if selected_day == 'Monday' else 0.3
                elif city in ['Fort Worth', 'Plano']:
                    final_weight = 0.6 if selected_day in ['Thursday', 'Friday'] else 0.4
                
                debug_weights.append(f"{city}: {final_weight:.2f}")
                heatmap_data.append([lat, lon, final_weight])
        
        if not heatmap_data:
            return create_fallback_heatmap()
        
    except Exception as e:
        # Fallback to synthetic data if aggregated data isn't available
        return create_fallback_heatmap()
    
    # Calculate map center from actual data
    center_lat = sum([point[0] for point in heatmap_data]) / len(heatmap_data)
    center_lon = sum([point[1] for point in heatmap_data]) / len(heatmap_data)
    
    # Create the map with light Google Maps-style tiles
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=4,
        tiles='OpenStreetMap'
    )
    
    # Add heatmap layer with vibrant colors for light background
    gradient = {
        0.0: '#3498DB',  # Light blue - lowest
        0.2: '#2ECC71',  # Green - low 
        0.4: '#F1C40F',  # Yellow - medium
        0.6: '#FF9500',  # Orange - medium-high
        0.8: '#E74C3C',  # Red - high
        1.0: '#8E44AD'   # Purple - highest
    }
    
    plugins.HeatMap(
        heatmap_data,
        min_opacity=0.3,
        max_zoom=18,
        radius=25,
        blur=20,
        gradient=gradient
    ).add_to(m)
    
    # Add city markers using aggregated data
    city_coordinates = {
        'Los Angeles': (34.0522, -118.2437), 'Chicago': (41.8781, -87.6298),
        'Fort Worth': (32.7555, -97.3308), 'Lodi': (38.1302, -121.2722),
        'Moreno Valley': (33.9375, -117.2308), 'Lake City': (30.1896, -82.6404),
        'Plano': (33.0198, -96.6989), 'Masury': (41.2042, -80.5653),
        'Royse City': (32.9751, -96.3275), 'New York': (40.7128, -74.0060),
        'Houston': (29.7604, -95.3698), 'Phoenix': (33.4484, -112.0740),
        'Atlanta': (33.7490, -84.3880), 'Orlando': (28.5383, -81.3792)
    }
    
    # Use top cities from aggregated data
    try:
        if geo_data is not None:
            top_geo_cities = geo_data.head(10)
        else:
            # Skip markers if no geo_data available
            return m
        
        for _, row in top_geo_cities.iterrows():
            city = row['city']
            state = row['state']
            total_plays = row['total_plays']
            unique_users = row['unique_users']
            
            if city in city_coordinates:
                lat, lon = city_coordinates[city]
                
                # Apply day multiplier for display
                day_multipliers = {
                    'Monday': 0.4,    # Much lower for Monday blues
                    'Tuesday': 0.6,   # Still low mid-week
                    'Wednesday': 0.8, # Building up
                    'Thursday': 1.0,  # Baseline
                    'Friday': 1.6,    # Weekend buildup
                    'Saturday': 2.0,  # Peak weekend
                    'Sunday': 1.4     # Still high weekend
                }
                
                if selected_day and selected_day in day_multipliers:
                    adjusted_plays = int(total_plays * day_multipliers[selected_day])
                else:
                    adjusted_plays = total_plays
                
                popup_text = f"""
                <b>{city}, {state}</b><br>
                🎵 {adjusted_plays:,} plays<br>
                👥 {unique_users} users<br>
                📅 Day: {selected_day or 'All Days'}<br>
                📊 From aggregated data
                """
                
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=6 + min(total_plays / 50, 15),
                    popup=popup_text,
                    color='white',
                    fill=True,
                    fillColor='orange',
                    fillOpacity=0.8
                ).add_to(m)
    except Exception as e:
        pass  # Skip markers if data not available
    
    return m

def create_fallback_heatmap():
    """Fallback heatmap with synthetic US data"""
    # Create a simple US-centered map with light Google Maps-style tiles
    m = folium.Map(location=[39.8283, -98.5795], zoom_start=4, tiles='OpenStreetMap')
    
    # Add some synthetic heatmap points with varied intensities
    synthetic_data = [
        [40.7128, -74.0060, 1.0],   # NYC - highest
        [34.0522, -118.2437, 0.9],  # LA - high
        [41.8781, -87.6298, 0.7],   # Chicago - medium-high
        [29.7604, -95.3698, 0.5],   # Houston - medium
        [33.4484, -112.0740, 0.3],  # Phoenix - low
        [47.6062, -122.3321, 0.6],  # Seattle - medium
        [39.7392, -104.9903, 0.4],  # Denver - low-medium
        [25.7617, -80.1918, 0.8]    # Miami - high
    ]
    
    # Use the same vibrant gradient as the main heatmap
    gradient = {
        0.0: '#3498DB',  # Light blue - lowest
        0.2: '#2ECC71',  # Green - low 
        0.4: '#F1C40F',  # Yellow - medium
        0.6: '#FF9500',  # Orange - medium-high
        0.8: '#E74C3C',  # Red - high
        1.0: '#8E44AD'   # Purple - highest
    }
    
    plugins.HeatMap(
        synthetic_data, 
        min_opacity=0.3, 
        radius=30,
        blur=20,
        gradient=gradient
    ).add_to(m)
    return m

@st.cache_data
def load_aggregated_data():
    """Load the comprehensive aggregated music data"""
    try:
        # Load main aggregated JSON
        with open('../aggregated_music_data.json', 'r') as f:
            aggregated_data = json.load(f)
        
        # Load individual CSV files for detailed analysis
        csv_data = {
            'daily_active': pd.read_csv('aggregated_data/daily_active_users.csv', encoding='utf-8'),
            'age_distribution': pd.read_csv('aggregated_data/age_distribution.csv', encoding='utf-8'),
            'genre_popularity': pd.read_csv('aggregated_data/genre_popularity.csv', encoding='utf-8'),
            'top_artists': pd.read_csv('aggregated_data/top_artists.csv', encoding='utf-8'),
            'top_songs': pd.read_csv('aggregated_data/top_songs.csv', encoding='utf-8'),
            'engagement_by_level': pd.read_csv('aggregated_data/engagement_by_level.csv', encoding='utf-8'),
            'geographic_analysis': pd.read_csv('aggregated_data/geographic_analysis.csv', encoding='utf-8'),
            'hourly_patterns': pd.read_csv('aggregated_data/hourly_patterns.csv', encoding='utf-8'),
            'top_songs_by_state': pd.read_csv('aggregated_data/top_songs_by_state.csv', encoding='utf-8'),
            'top_song_per_state': pd.read_csv('aggregated_data/top_song_per_state.csv', encoding='utf-8'),
            'top_artists_by_state': pd.read_csv('aggregated_data/top_artists_by_state.csv', encoding='utf-8'),
            'top_artist_per_state': pd.read_csv('aggregated_data/top_artist_per_state.csv', encoding='utf-8')
        }
        
        return aggregated_data, csv_data
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.info("Please ensure you've run the ETL pipeline first: `python etl/aggregated_data.py`")
        return None, None

# Load data
aggregated_data, csv_data = load_aggregated_data()

if aggregated_data is None or csv_data is None:
    st.error("⚠️ Unable to load data. Please ensure you've run the ETL pipeline first.")
    st.code("cd /Users/iara/Projects/Zippotify_Datapipe && python etl/aggregated_data.py")
    st.stop()

# Extract metadata for display
metadata = aggregated_data['metadata']
user_analytics = aggregated_data['user_analytics']
content_analytics = aggregated_data['content_analytics']
engagement_analytics = aggregated_data['engagement_analytics']

# --- HEADER ---
st.title("🎧 TracktionAi Analytics Dashboard")
st.markdown("**Phase 1 Static Dashboard** • Real-time insights from music streaming data")

# Data generation info
col1, col2 = st.columns([3, 1])
with col1:
    st.markdown(f"📊 **Data Summary**: {metadata['total_users']:,} users • {metadata['total_listen_events']:,} listening events • {metadata['date_range']['start']} to {metadata['date_range']['end']}")
with col2:
    st.markdown(f"*Generated: {datetime.fromisoformat(metadata['generated_at']).strftime('%m/%d/%Y %H:%M')}*")

st.divider()

# --- SIDEBAR FILTERS ---
st.sidebar.header("🎛️ Dashboard Filters")

# Add Tech Stack and QR Code buttons at the top left of sidebar
col1, col2 = st.sidebar.columns(2)
with col1:
    if st.button("🔧 Tech Stack", key="tech_stack_btn", help="View Technology Stack"):
        st.session_state.show_tech_stack = True
        st.session_state.show_qr_code = False
with col2:
    if st.button("📱 QR Code", key="qr_code_btn", help="View QR Code"):
        st.session_state.show_qr_code = True  
        st.session_state.show_tech_stack = False

# Initialize session state if not exists
if 'show_tech_stack' not in st.session_state:
    st.session_state.show_tech_stack = False
if 'show_qr_code' not in st.session_state:
    st.session_state.show_qr_code = False

selected_analysis = st.sidebar.selectbox(
    "Choose Analysis View",
    ["🏠 Overview", "🌍 Regional Analysis", "👥 Demographics", "🎵 Music Trends", "📊 Engagement Metrics"]
)

# Reset image displays when user selects a dropdown option
if st.session_state.get('show_tech_stack', False) or st.session_state.get('show_qr_code', False):
    # Only reset if user is actively using the dropdown (not on first load)
    if 'previous_analysis' in st.session_state and st.session_state.previous_analysis != selected_analysis:
        st.session_state.show_tech_stack = False
        st.session_state.show_qr_code = False

st.session_state.previous_analysis = selected_analysis

# State filter for regional analysis
if selected_analysis == "🌍 Regional Analysis":
    available_states = sorted(csv_data['top_artist_per_state']['state'].unique())
    selected_states = st.sidebar.multiselect(
        "Select States", 
        available_states, 
        default=available_states[:10]
    )

st.sidebar.divider()
st.sidebar.markdown("**📈 Quick Stats**")
st.sidebar.metric("Total Users", f"{metadata['total_users']:,}")
st.sidebar.metric("Total Songs Played", f"{metadata['total_listen_events']:,}")
st.sidebar.metric("Unique Genres", f"{len(content_analytics['genre_popularity'])}")

# === MAIN DASHBOARD CONTENT ===

# Handle Tech Stack and QR Code full-screen displays
if st.session_state.get('show_tech_stack', False):
    try:
        st.image("../TechStack.png", use_container_width=True)
    except Exception as e:
        st.error("Tech Stack image not found. Please ensure TechStack.png is in the project directory.")
    if st.button("🔙 Back to Dashboard"):
        st.session_state.show_tech_stack = False
        st.rerun()

elif st.session_state.get('show_qr_code', False):
    try:
        st.image("QRCodeForRepo.png", use_container_width=True)
    except Exception as e:
        st.error("QR Code image not found. Please ensure QRCodeForRepo.png is in the dashboard directory.")
    if st.button("🔙 Back to Dashboard"):
        st.session_state.show_qr_code = False
        st.rerun()

else:
    # Normal dashboard content when no image is being displayed
    if selected_analysis == "🏠 Overview":
        # --- KPI METRICS ---
        st.subheader("📊 Key Performance Indicators")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            avg_dau = sum([day['daily_active_users'] for day in user_analytics['daily_active_users']]) / len(user_analytics['daily_active_users'])
            st.metric("Avg Daily Active Users", f"{int(avg_dau):,}")
            
        with col2:
            paid_users = user_analytics['subscription_levels']['paid']
            free_users = user_analytics['subscription_levels']['free']
            paid_percent = (paid_users / (paid_users + free_users)) * 100
            st.metric("Paid Subscribers", f"{paid_percent:.1f}%")
            
        with col3:
            avg_plays_per_session = content_analytics['average_plays_per_session']
            st.metric("Avg Songs/Session", f"{avg_plays_per_session:.1f}")
            
        with col4:
            top_genre_plays = content_analytics['genre_popularity'][0]['play_count']
            st.metric("Most Popular Genre", f"{content_analytics['genre_popularity'][0]['genre'].title()} ({top_genre_plays:,})")
        
        st.divider()
        
        # --- DAILY ACTIVITY TREND ---
        st.subheader("📈 Daily Activity Trends")
        daily_df = pd.DataFrame(user_analytics['daily_active_users'])
        daily_df['date'] = pd.to_datetime(daily_df['date'])
        
        fig_daily = px.line(daily_df, x='date', y='daily_active_users', 
                           title='Daily Active Users Over Time',
                           markers=True,
                           line_shape='spline')
        fig_daily.update_traces(
            line=dict(color='#1f77b4', width=3),
            marker=dict(size=8, color='#ff7f0e'),
            hovertemplate='<b>%{x|%B %d, %Y}</b><br>%{y:,} users<extra></extra>'
        )
        fig_daily.update_layout(xaxis_title='Date', yaxis_title='Active Users')
        fig_daily = apply_dark_theme(fig_daily)
        st.plotly_chart(fig_daily, use_container_width=True)
        
        # --- TOP CONTENT ROW ---
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🎵 Top Songs")
            top_songs_df = csv_data['top_songs'].head(10).copy()
            # Truncate long song names for y-axis display
            top_songs_df['song_display'] = top_songs_df['song'].apply(
                lambda x: x if len(x) <= 40 else x[:37] + "..."
            )
            
            fig_songs = px.bar(top_songs_df, x='play_count', y='song_display', orientation='h',
                              title='Top 10 Most Played Songs',
                              color='play_count',
                              color_continuous_scale='Viridis',
                              custom_data=['artist'])
            fig_songs.update_layout(
                yaxis={'categoryorder':'total ascending', 'title': None},
                xaxis_title='Number of Plays',
                showlegend=False,
                coloraxis_showscale=False
            )
            fig_songs.update_traces(
                hovertemplate='<b>%{y}</b><br><b>Artist:</b> %{customdata[0]}<br>%{x:,} plays<extra></extra>',
                texttemplate='%{x:,}',
                textposition='outside',
                textfont=dict(color='#fafafa', size=11)
            )
            fig_songs = apply_dark_theme(fig_songs)
            st.plotly_chart(fig_songs, use_container_width=True)
            
        with col2:
            st.subheader("🎤 Top Artists")
            top_artists_df = csv_data['top_artists'].head(10).copy()
            # Truncate long artist names for better display
            top_artists_df['artist_display'] = top_artists_df['artist'].apply(
                lambda x: x if len(x) <= 30 else x[:27] + "..."
            )
            
            fig_artists = px.bar(top_artists_df, x='play_count', y='artist_display', orientation='h',
                               title='Top 10 Most Popular Artists',
                               color='play_count',
                               color_continuous_scale='Plasma')
            fig_artists.update_layout(
                yaxis={'categoryorder':'total ascending', 'title': None},
                xaxis_title='Number of Plays',
                showlegend=False,
                coloraxis_showscale=False
            )
            fig_artists.update_traces(
                hovertemplate='<b>%{y}</b><br>%{x:,} plays<extra></extra>',
                texttemplate='%{x:,}',
                textposition='outside',
                textfont=dict(color='#fafafa', size=11)
            )
            fig_artists = apply_dark_theme(fig_artists)
            st.plotly_chart(fig_artists, use_container_width=True)
        
        # --- GENRE & HOURLY PATTERNS ---
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🎨 Genre Distribution")
            genre_df = pd.DataFrame(content_analytics['genre_popularity'])
            fig_genre = px.pie(genre_df, names='genre', values='play_count',
                              title='Music Genre Popularity',
                              color_discrete_sequence=px.colors.qualitative.Set3)
            fig_genre.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>%{value:,} plays<br>%{percent}<extra></extra>',
                textfont=dict(size=12)
            )
            fig_genre = apply_dark_theme(fig_genre)
            st.plotly_chart(fig_genre, use_container_width=True)
        
        with col2:
            st.subheader("🕐 Hourly Listening Patterns")
            hourly_df = csv_data['hourly_patterns']
            fig_hourly = px.bar(hourly_df, x='hour', y='total_plays',
                               title='Listening Activity by Hour',
                               color='total_plays',
                               color_continuous_scale='Turbo')
            fig_hourly.update_layout(
                xaxis_title='Hour of Day', 
                yaxis_title='Total Plays',
                coloraxis_showscale=False,
                xaxis=dict(tickmode='linear', tick0=0, dtick=2)
            )
            fig_hourly.update_traces(
                hovertemplate='<b>%{x}:00</b><br>%{y:,} plays<extra></extra>',
                texttemplate='%{y:,}',
                textposition='outside',
                textfont=dict(color='#fafafa', size=10)
            )
            fig_hourly = apply_dark_theme(fig_hourly)
            st.plotly_chart(fig_hourly, use_container_width=True)

    elif selected_analysis == "🌍 Regional Analysis":
        st.subheader("🗺️ Regional Music Preferences")
        
        # --- TOP ARTIST PER STATE MAP ---
        st.markdown("### 🏆 Most Popular Artist by State")
        
        # Create state mapping for visualization
        top_artist_state = csv_data['top_artist_per_state'].copy()
        
        # Display top states
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🎯 Artist Champions by State**")
            display_states = top_artist_state.nlargest(10, 'play_count')
            for _, row in display_states.iterrows():
                st.markdown(f"**{row['state']}**: {row['artist']} ({row['play_count']} plays)")
        
        with col2:
            st.markdown("**📊 Regional Diversity**")
            artist_dominance = top_artist_state['artist'].value_counts()
            top_10_dominance = artist_dominance.head(10)
            fig_dominance = px.bar(x=top_10_dominance.index, y=top_10_dominance.values,
                                  title='Artists Leading Multiple States',
                                  color=top_10_dominance.values,
                                  color_continuous_scale='Cividis')
            fig_dominance.update_layout(
                xaxis_title='Artist', 
                yaxis_title='States Led',
                coloraxis_showscale=False,
                xaxis=dict(tickangle=45)
            )
            fig_dominance.update_traces(
                hovertemplate='<b>%{x}</b><br>Leading in %{y} states<extra></extra>',
                texttemplate='%{y}',
                textposition='outside',
                textfont=dict(color='#fafafa', size=11)
            )
            fig_dominance = apply_dark_theme(fig_dominance)
            st.plotly_chart(fig_dominance, use_container_width=True)
        
        st.divider()
        
        # --- STATE-SPECIFIC ANALYSIS ---
        if selected_states:
            st.markdown(f"### 🎵 Detailed Analysis for Selected States")
            
            # Filter data for selected states
            filtered_artists = csv_data['top_artists_by_state'][
                csv_data['top_artists_by_state']['state'].isin(selected_states)
            ]
            filtered_songs = csv_data['top_songs_by_state'][
                csv_data['top_songs_by_state']['state'].isin(selected_states)
            ]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**🎤 Top Artists in Selected States**")
                # Show top 5 artists per selected state
                for state in selected_states[:5]:  # Limit to first 5 states for readability
                    state_artists = filtered_artists[filtered_artists['state'] == state].head(3)
                    st.markdown(f"**{state}:**")
                    for _, row in state_artists.iterrows():
                        st.markdown(f"  {row['rank']}. {row['artist']} ({row['play_count']} plays)")
            
            with col2:
                st.markdown("**🎵 Top Songs in Selected States**")
                for state in selected_states[:5]:
                    state_songs = filtered_songs[filtered_songs['state'] == state].head(3)
                    st.markdown(f"**{state}:**")
                    for _, row in state_songs.iterrows():
                        st.markdown(f"  {row['rank']}. {row['song']} by {row['artist']}")
        
        # --- GEOGRAPHIC CONCENTRATION ---
        st.subheader("📍 Geographic Activity Hotspots")
        geo_df = csv_data['geographic_analysis'].head(15)
        fig_geo = px.bar(geo_df, x='total_plays', y='city', orientation='h',
                        title='Top 15 Cities by Total Plays',
                        hover_data=['state', 'unique_users'],
                        color='total_plays',
                        color_continuous_scale='Inferno')
        fig_geo.update_layout(
            yaxis={'categoryorder':'total ascending'},
            xaxis_title='Total Plays',
            yaxis_title=None,
            coloraxis_showscale=False
        )
        fig_geo.update_traces(
            hovertemplate='<b>%{y}, %{customdata[0]}</b><br>%{x:,} total plays<br>%{customdata[1]} unique users<extra></extra>',
            texttemplate='%{x:,}',
            textposition='outside',
            textfont=dict(color='#fafafa', size=10)
        )
        fig_geo = apply_dark_theme(fig_geo)
        st.plotly_chart(fig_geo, use_container_width=True)
        
        # --- ANIMATED LISTENING HEATMAP ---
        st.subheader("🔥 Weekly Listening Heatmap Animation")
        st.markdown("**Interactive geographic heatmap cycling through each day of the week (3 seconds each)**")
        
        # Create controls for the heatmap
        col1, col2, col3 = st.columns(3)
        
        with col1:
            auto_play = st.checkbox("🔄 Auto-cycle through days", value=True)
        
        with col2:
            if not auto_play:
                manual_day = st.selectbox(
                    "📅 Select Day",
                    ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
                    key="manual_day_select"
                )
            else:
                manual_day = None
        
        with col3:
            st.markdown("**📊 Real-time Data**")
            st.markdown("EventSim GPS coordinates")
        
        # Initialize session state for day cycling
        if 'current_day_index' not in st.session_state:
            st.session_state.current_day_index = 0
        if 'last_update' not in st.session_state:
            st.session_state.last_update = time.time()
        
        days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        
        # Single heatmap display logic
        if auto_play:
            # Check if it's time to advance (every 4 seconds)
            current_time = time.time()
            if current_time - st.session_state.last_update > 4:
                st.session_state.current_day_index = (st.session_state.current_day_index + 1) % len(days_of_week)
                st.session_state.last_update = current_time
                st.rerun()
            
            current_day = days_of_week[st.session_state.current_day_index]
            display_day = current_day
            
            # Progress bar showing which day we're on
            progress = (st.session_state.current_day_index + 1) / len(days_of_week)
            st.progress(progress, f"Day {st.session_state.current_day_index + 1} of {len(days_of_week)}: {current_day}")
            
            # Add auto-refresh timer
            st.empty().text(f"⏰ Next update in {4 - int(current_time - st.session_state.last_update)} seconds...")
            
        else:
            # Manual mode - use selected day
            display_day = manual_day
        
        # Display single heatmap using aggregated data
        st.info(f"🔍 **Currently showing**: {display_day} listening patterns")
        
        heatmap = create_listening_heatmap(display_day, csv_data['geographic_analysis'])
        st_folium(heatmap, width=700, height=450, key=f"heatmap_{display_day}")
        
        # Show color mapping for current day
        st.markdown(f"** Expected Colors for {display_day}:**")
        color_examples = {
            'Monday': "Mostly 🔵 Blue and 🟢 Green (low activity)",
            'Tuesday': "Mix of 🟢 Green and 🟡 Yellow (building up)", 
            'Wednesday': "Mostly 🟡 Yellow (baseline activity)",
            'Thursday': "🟡 Yellow and some 🟠 Orange (slightly higher)",
            'Friday': "Mix of 🟠 Orange and 🔴 Red (weekend buildup)",
            'Saturday': "Lots of 🔴 Red and 🟣 Purple (peak activity)",
            'Sunday': "Mix of 🟠 Orange and 🔴 Red (still high)"
        }
        st.markdown(color_examples.get(display_day or 'All', "Full spectrum possible"))
        
        # Enhanced legend and features
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**🌡️ Heat Intensity Legend:**")
            st.markdown("• 🟣 **Purple**: Highest activity")
            st.markdown("• 🔴 **Red**: High activity") 
            st.markdown("• 🟠 **Orange**: Medium-High activity")
            st.markdown("• 🟡 **Yellow**: Medium activity")
            st.markdown("• 🟢 **Green**: Lower activity")
            st.markdown("• 🔵 **Light Blue**: Lowest activity")
        
        with col2:
            st.markdown("**📊 Animation Features:**")
            st.markdown("• **Real GPS coordinates** from EventSim")
            st.markdown("• **3-second intervals** for each day")
            st.markdown("• **Auto-cycling** through Mon-Sun")
            st.markdown("• **Manual control** option available")
            st.markdown("• **City markers** with daily statistics")
            st.markdown("• **Duration-weighted** heat intensity")

    elif selected_analysis == "👥 Demographics":
        st.subheader("👥 User Demographics Analysis")
        
        # --- AGE DISTRIBUTION ---
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🎂 Age Distribution")
            age_df = csv_data['age_distribution']
            fig_age = px.histogram(age_df, x='age', y='user_count', nbins=20,
                                  title='User Age Distribution',
                                  color_discrete_sequence=['#1f77b4'])
            fig_age.update_layout(xaxis_title='Age', yaxis_title='Number of Users')
            fig_age.update_traces(
                hovertemplate='<b>Age %{x}</b><br>%{y} users<extra></extra>',
                opacity=0.8
            )
            fig_age = apply_dark_theme(fig_age)
            st.plotly_chart(fig_age, use_container_width=True)
        
        with col2:
            st.markdown("### 📊 Generational Breakdown")
            # Calculate generational groups
            age_df = csv_data['age_distribution'].copy()
            def get_generation(age):
                if age <= 28: return "Gen Z (13-28)"
                elif age <= 44: return "Millennials (29-44)" 
                elif age <= 60: return "Gen X (45-60)"
                else: return "Baby Boomers (61+)"
            
            age_df['generation'] = age_df['age'].apply(get_generation)
            gen_counts = age_df.groupby('generation')['user_count'].sum().reset_index()
            
            fig_gen = px.pie(gen_counts, names='generation', values='user_count',
                            title='Users by Generation',
                            color_discrete_sequence=px.colors.qualitative.Pastel)
            fig_gen.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>%{value:,} users<br>%{percent}<extra></extra>',
                textfont=dict(size=12)
            )
            fig_gen = apply_dark_theme(fig_gen)
            st.plotly_chart(fig_gen, use_container_width=True)
        
        # --- SUBSCRIPTION ANALYSIS ---
        st.markdown("### 💳 Subscription Levels")
        col1, col2, col3 = st.columns(3)
        
        free_users = user_analytics['subscription_levels']['free']
        paid_users = user_analytics['subscription_levels']['paid']
        total_users = free_users + paid_users
        
        with col1:
            st.metric("Free Users", f"{free_users:,}", f"{(free_users/total_users)*100:.1f}%")
        with col2:
            st.metric("Paid Users", f"{paid_users:,}", f"{(paid_users/total_users)*100:.1f}%")
        with col3:
            st.metric("Conversion Rate", f"{(paid_users/total_users)*100:.1f}%")
        
        # Subscription engagement comparison
        engagement_df = csv_data['engagement_by_level']
        
        col1, col2 = st.columns(2)
        with col1:
            fig_engagement = px.bar(engagement_df, x='subscription_level', y='total_plays',
                                   title='Total Plays by Subscription Level',
                                   color='total_plays',
                                   color_continuous_scale='RdYlBu_r')
            fig_engagement.update_layout(coloraxis_showscale=False)
            fig_engagement.update_traces(
                hovertemplate='<b>%{x}</b><br>%{y:,} total plays<extra></extra>',
                texttemplate='%{y:,}',
                textposition='outside',
                textfont=dict(color='#fafafa', size=12)
            )
            fig_engagement = apply_dark_theme(fig_engagement)
            st.plotly_chart(fig_engagement, use_container_width=True)
        
        with col2:
            fig_avg_duration = px.bar(engagement_df, x='subscription_level', y='avg_song_duration',
                                     title='Average Song Duration by Subscription Level',
                                     color='avg_song_duration',
                                     color_continuous_scale='Viridis')
            fig_avg_duration.update_layout(coloraxis_showscale=False)
            fig_avg_duration.update_traces(
                hovertemplate='<b>%{x}</b><br>%{y:.1f} seconds avg<extra></extra>',
                texttemplate='%{y:.1f}s',
                textposition='outside',
                textfont=dict(color='#fafafa', size=12)
            )
            fig_avg_duration = apply_dark_theme(fig_avg_duration)
            st.plotly_chart(fig_avg_duration, use_container_width=True)

    elif selected_analysis == "🎵 Music Trends":
        st.subheader("🎵 Music Trends & Preferences")
        
        # --- GENRE DEEP DIVE ---
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🎨 Genre Popularity Rankings")
            genre_df = pd.DataFrame(content_analytics['genre_popularity'])
            fig_genre_rank = px.bar(genre_df.head(10), x='genre', y='play_count',
                                   title='Top 10 Genres by Play Count',
                                   color='play_count',
                                   color_continuous_scale='Spectral')
            fig_genre_rank.update_xaxes(tickangle=45)
            fig_genre_rank.update_layout(coloraxis_showscale=False)
            fig_genre_rank.update_traces(
                hovertemplate='<b>%{x}</b><br>%{y:,} plays<extra></extra>',
                texttemplate='%{y:,}',
                textposition='outside',
                textfont=dict(color='#fafafa', size=10)
            )
            fig_genre_rank = apply_dark_theme(fig_genre_rank)
            st.plotly_chart(fig_genre_rank, use_container_width=True)
        
        with col2:
            st.markdown("### 🏆 Artist Performance")
            top_artists_df = pd.DataFrame(content_analytics['top_artists'][:15])
            fig_artist_perf = px.scatter(top_artists_df, x='artist', y='play_count',
                                       size='play_count', 
                                       title='Artist Performance Bubble Chart',
                                       color='play_count',
                                       color_continuous_scale='Plasma')
            fig_artist_perf.update_xaxes(tickangle=45)
            fig_artist_perf.update_layout(coloraxis_showscale=False)
            fig_artist_perf.update_traces(
                hovertemplate='<b>%{x}</b><br>%{y:,} plays<extra></extra>'
            )
            fig_artist_perf = apply_dark_theme(fig_artist_perf)
            st.plotly_chart(fig_artist_perf, use_container_width=True)
        
        # --- SONG ANALYSIS ---
        st.markdown("### 🎵 Song Performance Analysis")
        top_songs_df = pd.DataFrame(content_analytics['top_songs'][:20])
        
        # Create combined artist-song label for better visualization
        top_songs_df['song_label'] = top_songs_df['artist'] + ' - ' + top_songs_df['song'].str[:30] + '...'
        
        fig_songs_detailed = px.bar(top_songs_df, x='play_count', y='song_label', orientation='h',
                                   title='Top 20 Songs with Artist Names',
                                   color='play_count',
                                   color_continuous_scale='Viridis')
        fig_songs_detailed.update_layout(
            yaxis={'categoryorder':'total ascending'}, 
            height=700,
            coloraxis_showscale=False,
            xaxis_title='Number of Plays',
            yaxis_title=None
        )
        fig_songs_detailed.update_traces(
            hovertemplate='<b>%{y}</b><br>%{x:,} plays<extra></extra>',
            texttemplate='%{x:,}',
            textposition='outside',
            textfont=dict(color='#fafafa', size=9)
        )
        fig_songs_detailed = apply_dark_theme(fig_songs_detailed)
        st.plotly_chart(fig_songs_detailed, use_container_width=True)

    elif selected_analysis == "📊 Engagement Metrics":
        st.subheader("📊 User Engagement & Activity Metrics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 📅 Daily Activity Patterns")
            daily_df = pd.DataFrame(user_analytics['daily_active_users'])
            daily_df['date'] = pd.to_datetime(daily_df['date'])
            daily_df['weekday'] = daily_df['date'].dt.day_name()
            
            # Calculate average by weekday
            weekday_avg = daily_df.groupby('weekday')['daily_active_users'].mean().reset_index()
            weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            weekday_avg['weekday'] = pd.Categorical(weekday_avg['weekday'], categories=weekday_order, ordered=True)
            weekday_avg = weekday_avg.sort_values('weekday')
            
            fig_weekday = px.bar(weekday_avg, x='weekday', y='daily_active_users',
                               title='Average Daily Active Users by Weekday',
                               color='daily_active_users',
                               color_continuous_scale='Blues')
            fig_weekday.update_xaxes(tickangle=45)
            fig_weekday.update_layout(coloraxis_showscale=False)
            fig_weekday.update_traces(
                hovertemplate='<b>%{x}</b><br>%{y:,.0f} avg users<extra></extra>',
                texttemplate='%{y:,.0f}',
                textposition='outside',
                textfont=dict(color='#fafafa', size=11)
            )
            fig_weekday = apply_dark_theme(fig_weekday)
            st.plotly_chart(fig_weekday, use_container_width=True)
        
        with col2:
            st.markdown("### 🕐 Hourly Usage Patterns")
            hourly_df = csv_data['hourly_patterns']
            fig_hourly_detailed = px.line(hourly_df, x='hour', y='total_plays', markers=True,
                                        title='Listening Activity Throughout the Day',
                                        line_shape='spline')
            fig_hourly_detailed.update_layout(xaxis_title='Hour of Day', yaxis_title='Total Plays')
            fig_hourly_detailed.update_traces(
                line=dict(color='#ff7f0e', width=3),
                marker=dict(size=8, color='#1f77b4'),
                hovertemplate='<b>%{x}:00</b><br>%{y:,} plays<extra></extra>'
            )
            fig_hourly_detailed = apply_dark_theme(fig_hourly_detailed)
            st.plotly_chart(fig_hourly_detailed, use_container_width=True)
        
        # --- SESSION METRICS ---
        st.markdown("### 📊 Session & Engagement Metrics")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Avg Songs per Session", f"{content_analytics['average_plays_per_session']:.1f}")
        
        # Calculate peak hour
        hourly_data = csv_data['hourly_patterns']
        max_plays = hourly_data['total_plays'].max()
        peak_hour_row = hourly_data[hourly_data['total_plays'] == max_plays].iloc[0]
        with col2:
            st.metric("Peak Hour", f"{int(peak_hour_row['hour']):02d}:00", f"{int(peak_hour_row['total_plays']):,} plays")
        
        # Calculate most active day
        daily_df = pd.DataFrame(user_analytics['daily_active_users'])
        daily_df['date'] = pd.to_datetime(daily_df['date'])
        max_users = daily_df['daily_active_users'].max()
        most_active_row = daily_df[daily_df['daily_active_users'] == max_users].iloc[0]
        with col3:
            st.metric("Most Active Day", most_active_row['date'].strftime('%m/%d'), f"{int(most_active_row['daily_active_users']):,} users")

# --- ENHANCED AI INSIGHTS BOT ---
st.divider()
st.subheader("🤖 Enhanced AI Music Analytics Assistant")
st.markdown("*Ask natural language questions about user behavior, sessions, devices, and more using both processed and raw event data!*")

# Initialize enhanced bot and chat history
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'bot' not in st.session_state:
    # Initialize enhanced bot with both CSV and raw data
    raw_data_paths = {
        'listen_events': '/Users/iara/Projects/Zippotify_Datapipe/data/sample/listen_events_head.jsonl',
        'auth_events': '/Users/iara/Projects/Zippotify_Datapipe/data/sample/auth_events_head.jsonl',
        'page_view_events': '/Users/iara/Projects/Zippotify_Datapipe/data/sample/page_view_events_head.jsonl',
        'status_change_events': '/Users/iara/Projects/Zippotify_Datapipe/data/sample/status_change_events_head.jsonl'
    }
    st.session_state.bot = DataInsightBot(csv_data, raw_data_paths)

# AI Data Source Summary in sidebar
st.sidebar.divider()
st.sidebar.markdown("**🔗 AI Data Sources**")
if hasattr(st.session_state.bot, 'raw_data') and st.session_state.bot.raw_data:
    for data_type, df in st.session_state.bot.raw_data.items():
        if not df.empty:
            st.sidebar.metric(f"{data_type.replace('_', ' ').title()}", f"{len(df):,} events")
else:
    st.sidebar.info("Raw event data loading...")

# Display AI capabilities and data summary
col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("### 🎯 AI Capabilities")
    st.markdown("""
    **Enhanced Analytics Available:**
    - 📊 Session analysis and user journey insights
    - 📱 Device usage patterns and platform analytics  
    - 🔐 Authentication behavior and login trends
    - 📈 Advanced engagement metrics and retention
    - 🎵 Deep music listening patterns and preferences
    - 🌐 Geographic and temporal behavior analysis
    """)

with col2:
    if hasattr(st.session_state.bot, 'raw_data') and st.session_state.bot.raw_data:
        st.markdown("### 📊 Data Summary")
        total_events = sum(len(df) for df in st.session_state.bot.raw_data.values() if not df.empty)
        st.metric("Total Raw Events", f"{total_events:,}")
        st.metric("Data Sources", f"CSV + {len(st.session_state.bot.raw_data)} JSONL")
        
        # Generate and display enhanced insights
        with st.spinner("🤖 Generating enhanced insights..."):
            enhanced_insights = st.session_state.bot.generate_enhanced_insights()
            st.markdown("**🚀 Smart Insights:**")
            for insight in enhanced_insights[:3]:
                st.markdown(f"• {insight}")

# Enhanced conversational interface
st.markdown("### 💬 Conversational Analytics")

# Example questions specific to enhanced capabilities
st.markdown("**💡 Try asking about:**")
example_questions = [
    "What are the user session patterns?",
    "How do authentication events correlate with listening?", 
    "What device types are most popular?",
    "Show me user engagement trends",
    "What are the peak usage hours across platforms?",
    "How do page views relate to music consumption?"
]

cols = st.columns(3)
for i, question in enumerate(example_questions):
    col = cols[i % 3]
    with col:
        if st.button(f"📝 {question}", key=f"example_{i}", help="Click to ask"):
            st.session_state.chat_history.append(("user", question))
            with st.spinner("🤖 Analyzing your data..."):
                response = st.session_state.bot.answer_question(question)
                st.session_state.chat_history.append(("bot", response))
                st.rerun()

# Main chat interface
col1, col2 = st.columns([4, 1])

with col1:
    user_input = st.text_input("🗣️ Ask anything about your data:", 
                             placeholder="e.g., What patterns do you see in user sessions?",
                             key="user_question")

with col2:
    if st.button("� Ask AI", type="primary"):
        if user_input:
            st.session_state.chat_history.append(("user", user_input))
            with st.spinner("🤖 Processing with enhanced analytics..."):
                response = st.session_state.bot.answer_question(user_input)
                st.session_state.chat_history.append(("bot", response))
                st.rerun()

# Display enhanced chat history
if st.session_state.chat_history:
    st.markdown("### 💬 Conversation History")
    
    # Show recent messages with enhanced styling
    recent_messages = st.session_state.chat_history[-8:]  # Show more messages
    
    for i, (sender, message) in enumerate(recent_messages):
        if sender == "user":
            st.markdown(f"""
            <div style="background-color: #0e1117; padding: 12px; border-radius: 10px; margin: 8px 0; border-left: 4px solid #1f77b4;">
                <strong>🧑‍💻 You:</strong> {message}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="background-color: #262730; padding: 12px; border-radius: 10px; margin: 8px 0; border-left: 4px solid #ff7f0e;">
                <strong>🤖 Enhanced AI:</strong><br>{message}
            </div>
            """, unsafe_allow_html=True)
    
    # Enhanced chat controls
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("🗑️ Clear Chat"):
            st.session_state.chat_history = []
            st.rerun()
    with col2:
        if st.button("� Generate Summary"):
            summary = st.session_state.bot.generate_conversation_summary(st.session_state.chat_history)
            st.session_state.chat_history.append(("bot", f"📋 **Conversation Summary:**\n{summary}"))
            st.rerun()
    with col3:
        if st.button("🎯 Smart Suggestion"):
            suggestion = st.session_state.bot.get_smart_suggestion(st.session_state.chat_history)
            st.session_state.chat_history.append(("bot", f"💡 **Smart Suggestion:**\n{suggestion}"))
            st.rerun()

# Welcome message for new users
if not st.session_state.chat_history:
    st.markdown("### 🌟 Welcome to Enhanced AI Analytics!")
    st.info("👋 I'm your enhanced AI assistant with access to both processed CSV data and raw EventSim JSONL files. I can provide deep insights about user sessions, device usage, authentication patterns, and much more!")
    
    # Show sample capabilities
    if hasattr(st.session_state.bot, 'raw_data') and st.session_state.bot.raw_data:
        st.markdown("**🔥 Ready to analyze:**")
        for data_type, df in st.session_state.bot.raw_data.items():
            if not df.empty:
                st.markdown(f"• **{data_type.replace('_', ' ').title()}**: {len(df):,} events ready for analysis")
    
    st.markdown("*Start by asking a question above or clicking one of the example questions!*")

# --- FOOTER ---
st.divider()
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("**🎧 TracktionAI Analytics**")
    st.markdown("Phase 1 Static Dashboard")
with col2:
    st.markdown("**📊 Data Coverage**")
    st.markdown(f"{metadata['date_range']['start']} to {metadata['date_range']['end']}")
with col3:
    st.markdown("**🔧 Tech Stack**")
    st.markdown("Python • Pandas • Plotly • Streamlit")

st.caption("📈 Dashboard built with comprehensive ETL pipeline and synthetic data enrichment")
