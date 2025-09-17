import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import json
from datetime import datetime

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

@st.cache_data
def load_aggregated_data():
    """Load the comprehensive aggregated music data"""
    try:
        # Load main aggregated JSON
        with open('../aggregated_music_data.json', 'r') as f:
            aggregated_data = json.load(f)
        
        # Load individual CSV files for detailed analysis
        csv_data = {
            'daily_active': pd.read_csv('../daily_active_users.csv'),
            'age_distribution': pd.read_csv('../age_distribution.csv'),
            'genre_popularity': pd.read_csv('../genre_popularity.csv'),
            'top_artists': pd.read_csv('../top_artists.csv'),
            'top_songs': pd.read_csv('../top_songs.csv'),
            'engagement_by_level': pd.read_csv('../engagement_by_level.csv'),
            'geographic_analysis': pd.read_csv('../geographic_analysis.csv'),
            'hourly_patterns': pd.read_csv('../hourly_patterns.csv'),
            'top_songs_by_state': pd.read_csv('../top_songs_by_state.csv'),
            'top_song_per_state': pd.read_csv('../top_song_per_state.csv'),
            'top_artists_by_state': pd.read_csv('../top_artists_by_state.csv'),
            'top_artist_per_state': pd.read_csv('../top_artist_per_state.csv')
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
selected_analysis = st.sidebar.selectbox(
    "Choose Analysis View",
    ["🏠 Overview", "🌍 Regional Analysis", "👥 Demographics", "🎵 Music Trends", "📊 Engagement Metrics"]
)

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
        top_songs_df = pd.DataFrame(content_analytics['top_songs'][:10])
        # Truncate long song names for better display
        top_songs_df['song_display'] = top_songs_df['song'].apply(
            lambda x: x if len(x) <= 35 else x[:32] + "..."
        )
        
        fig_songs = px.bar(top_songs_df, x='play_count', y='song_display', orientation='h',
                          title='Top 10 Most Played Songs',
                          color='play_count',
                          color_continuous_scale='Viridis')
        fig_songs.update_layout(
            yaxis={'categoryorder':'total ascending', 'title': None},
            xaxis_title='Number of Plays',
            showlegend=False,
            coloraxis_showscale=False
        )
        fig_songs.update_traces(
            hovertemplate='<b>%{y}</b><br>%{x:,} plays<extra></extra>',
            texttemplate='%{x:,}',
            textposition='outside',
            textfont=dict(color='#fafafa', size=11)
        )
        fig_songs = apply_dark_theme(fig_songs)
        st.plotly_chart(fig_songs, use_container_width=True)
        
    with col2:
        st.subheader("🎤 Top Artists")
        top_artists_df = pd.DataFrame(content_analytics['top_artists'][:10])
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
    
    # --- ACTIVITY PATTERNS ---
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
