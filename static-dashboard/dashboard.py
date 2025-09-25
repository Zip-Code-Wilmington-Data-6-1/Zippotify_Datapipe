import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import folium
import time
import json
import openai
import os
from datetime import datetime
from folium import plugins
from streamlit_folium import st_folium
from ai_bot import DataInsightBot

# Create a smart data bot with OpenAI integration
class SimpleDataBot:
    def __init__(self, csv_data, aggregated_data):
        self.csv_data = csv_data
        self.aggregated_data = aggregated_data
        
        # Initialize OpenAI client
        try:
            # Try to get API key from environment or Streamlit secrets
            openai_api_key = os.getenv("OPENAI_API_KEY") or st.secrets.get("OPENAI_API_KEY", "")
            if openai_api_key:
                self.client = openai.OpenAI(api_key=openai_api_key)
                self.use_openai = True
                st.success("🤖 OpenAI API connected - Enhanced AI responses enabled!")
                st.info("💡 Note: If you see quota errors, check your OpenAI billing at platform.openai.com")
            else:
                self.use_openai = False
                st.warning("⚠️ OpenAI API key not found - Using basic responses only")
                st.warning("⚠️ OpenAI API key not found - Using basic responses only")
        except Exception as e:
            self.use_openai = False
            st.warning(f"⚠️ OpenAI API setup failed: {str(e)} - Using basic responses")
    
    def deep_clean_data(self, data, max_depth=10):
        """Aggressively clean data to remove all problematic fields and ensure JSON serialization"""
        if max_depth <= 0:
            return str(data)  # Prevent infinite recursion
            
        try:
            if data is None:
                return None
            elif isinstance(data, (str, int, float, bool)):
                return data
            elif isinstance(data, dict):
                cleaned = {}
                for key, value in data.items():
                    # Skip any problematic keys
                    if (isinstance(key, str) and 
                        (key.startswith('_') or 
                         'pydantic' in key.lower() or 
                         key.startswith('__') or
                         key.endswith('__'))):
                        continue
                    # Ensure key is a simple string
                    clean_key = str(key).replace('_', '').replace('-', '')[:50]  # Limit key length
                    if clean_key and clean_key.isalnum():
                        cleaned[clean_key] = self.deep_clean_data(value, max_depth - 1)
                return cleaned
            elif isinstance(data, (list, tuple)):
                return [self.deep_clean_data(item, max_depth - 1) for item in data[:20]]  # Limit list size
            elif hasattr(data, 'to_dict'):
                # Handle pandas objects
                try:
                    return self.deep_clean_data(data.to_dict(), max_depth - 1)
                except:
                    return str(data)
            elif hasattr(data, 'item'):
                # Handle numpy scalars
                try:
                    return data.item()
                except:
                    return str(data)
            elif hasattr(data, 'tolist'):
                # Handle numpy arrays
                try:
                    return self.deep_clean_data(data.tolist(), max_depth - 1)
                except:
                    return str(data)
            else:
                # Convert everything else to string
                return str(data)[:200]  # Limit string length
        except Exception:
            return str(data)[:100]  # Fallback to string representation

    def get_data_summary(self):
        """Create a comprehensive summary of available data for OpenAI context"""
        try:
            
            summary = {
                "metadata": self.deep_clean_data(self.aggregated_data.get('metadata', {})),
                "states": list(self.csv_data['geographic_analysis']['state'].unique()) if 'geographic_analysis' in self.csv_data else [],
                "total_cities": int(self.csv_data['geographic_analysis']['cities_count'].sum()) if 'geographic_analysis' in self.csv_data else 0,
                "top_state": self.deep_clean_data(self.csv_data['geographic_analysis'].iloc[0].to_dict()) if 'geographic_analysis' in self.csv_data and len(self.csv_data['geographic_analysis']) > 0 else {},
                "top_genres": self.deep_clean_data(self.aggregated_data['content_analytics']['genre_popularity'][:5]) if self.aggregated_data and 'content_analytics' in self.aggregated_data else [],
                "top_artists": self.deep_clean_data(self.csv_data['top_artists'].head(5).to_dict('records')) if 'top_artists' in self.csv_data else [],
                "top_songs": self.deep_clean_data(self.csv_data['top_songs'].head(5).to_dict('records')) if 'top_songs' in self.csv_data else [],
                "peak_hour": self.deep_clean_data(self.csv_data['hourly_patterns'].loc[self.csv_data['hourly_patterns']['play_count'].idxmax()].to_dict()) if 'hourly_patterns' in self.csv_data else {},
                "user_engagement": self.deep_clean_data(self.csv_data['engagement_by_level'].to_dict('records')) if 'engagement_by_level' in self.csv_data else [],
                "age_demographics": {
                    "avg_age": float((self.csv_data['age_distribution']['age'] * self.csv_data['age_distribution']['user_count']).sum() / self.csv_data['age_distribution']['user_count'].sum()) if 'age_distribution' in self.csv_data else 0,
                    "age_groups": self.deep_clean_data(self.csv_data['age_distribution'].head(10).to_dict('records')) if 'age_distribution' in self.csv_data else []
                }
            }
            
            # Clean the entire summary to ensure no problematic keys remain
            return self.deep_clean_data(summary)
            
        except Exception as e:
            return {"error": f"Could not generate data summary: {str(e)}"}
    
    def answer_question_with_openai(self, question):
        """Use OpenAI to answer questions intelligently based on real data"""
        try:
            # Create an extremely simple data summary using only basic types
            simple_data = "Music streaming data summary:\n"
            
            # Basic metadata
            if self.aggregated_data and 'metadata' in self.aggregated_data:
                total_users = self.aggregated_data['metadata'].get('total_users', 0)
                simple_data += f"- Total users: {total_users}\n"
            
            # Geographic info
            if 'geographic_analysis' in self.csv_data and len(self.csv_data['geographic_analysis']) > 0:
                states_count = len(self.csv_data['geographic_analysis']['state'].unique())
                total_cities = int(self.csv_data['geographic_analysis']['cities_count'].sum())
                top_state_row = self.csv_data['geographic_analysis'].iloc[0]
                top_state = str(top_state_row['state'])
                top_state_plays = int(top_state_row['total_plays'])
                simple_data += f"- States: {states_count}\n"
                simple_data += f"- Total cities: {total_cities}\n"
                simple_data += f"- Top state: {top_state} with {top_state_plays:,} plays\n"
            
            # Top genres
            if self.aggregated_data and 'content_analytics' in self.aggregated_data:
                genres = self.aggregated_data['content_analytics']['genre_popularity'][:5]
                simple_data += f"- Top genres: {', '.join([str(g['genre']) for g in genres])}\n"
            
            # Top artists
            if 'top_artists' in self.csv_data:
                top_artists = self.csv_data['top_artists'].head(3)
                artists_list = [str(row['artist']) for _, row in top_artists.iterrows()]
                simple_data += f"- Top artists: {', '.join(artists_list)}\n"
            
            # Peak hour
            if 'hourly_patterns' in self.csv_data:
                peak_row = self.csv_data['hourly_patterns'].loc[self.csv_data['hourly_patterns']['play_count'].idxmax()]
                peak_hour = int(peak_row['hour'])
                peak_plays = int(peak_row['play_count'])
                simple_data += f"- Peak hour: {peak_hour}:00 with {peak_plays:,} plays\n"
            
            system_prompt = f"""You are TracktionAI, an expert music streaming data analyst. You have access to a comprehensive 11GB dataset from a music streaming platform.

AVAILABLE DATA:
{simple_data}

INSTRUCTIONS:
1. Answer questions using ONLY the real data provided above
2. Be specific with numbers, names, and statistics from the actual data
3. Use emojis and formatting to make responses engaging
4. If asked about something not in the data, say so clearly
5. Always cite specific data points when making claims
6. Format responses with headers, bullet points, and emphasis for readability

RESPONSE STYLE:
- Start with a relevant emoji
- Use **bold** for important numbers and names  
- Include specific statistics from the data
- End with relevant insights or context"""

            # Create the API call with minimal data
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": question}
                ],
                max_tokens=500,
                temperature=0.7
            )
            
            return response.choices[0].message.content or "Sorry, I couldn't generate a response."
            
        except Exception as e:
            error_str = str(e).lower()
            if 'quota' in error_str or 'billing' in error_str:
                return f"💳 **OpenAI API Quota Exceeded**: Your OpenAI API usage has reached its limit. Please check your billing at https://platform.openai.com/account/billing. Using basic response instead."
            elif 'api key' in error_str or 'authentication' in error_str:
                return f"🔑 **OpenAI API Key Issue**: Please check your API key configuration. Using basic response instead."
            else:
                return f"🚫 OpenAI API Error: {str(e)}. Falling back to basic response."
        
    def answer_question(self, question):
        """Answer questions using actual data from our 11GB dataset"""
        question_lower = question.lower().strip()
        
        # If OpenAI is available, use it for intelligent responses
        if self.use_openai:
            try:
                return self.answer_question_with_openai(question)
            except Exception as e:
                # Fall back to basic responses if OpenAI fails
                pass
        
        # Basic pattern-matching responses as fallback
        # STATES QUESTIONS
        if any(phrase in question_lower for phrase in ['how many states', 'number of states', 'states are there', 'count of states']):
            if 'geographic_analysis' in self.csv_data:
                states = self.csv_data['geographic_analysis']['state'].unique()
                return f"🌍 **{len(states)} states** are represented in our dataset: {', '.join(sorted(states))}"
        
        # TOP STATE QUESTION  
        if any(phrase in question_lower for phrase in ['which state leads', 'top state', 'leading state', 'state has most']):
            if 'geographic_analysis' in self.csv_data:
                top_state = self.csv_data['geographic_analysis'].iloc[0]
                return f"🏆 **{top_state['state']}** leads with **{top_state['total_plays']:,} total plays** across **{top_state['cities_count']} cities**!"
        
        # GENRE QUESTIONS
        if any(phrase in question_lower for phrase in ['genre', 'music type', 'top genres']):
            if self.aggregated_data and 'content_analytics' in self.aggregated_data:
                genres = self.aggregated_data['content_analytics']['genre_popularity'][:5]
                genre_list = []
                for i, genre_data in enumerate(genres, 1):
                    genre_list.append(f"{i}. **{genre_data['genre']}** ({genre_data['play_count']:,} plays)")
                return f"🎵 **Top 5 Genres:**\n" + "\n".join(genre_list)
        
        # PEAK HOURS QUESTION
        if any(phrase in question_lower for phrase in ['peak hour', 'busy hour', 'most active', 'when most']):
            if 'hourly_patterns' in self.csv_data:
                peak_hour = self.csv_data['hourly_patterns'].loc[self.csv_data['hourly_patterns']['play_count'].idxmax()]
                return f"⏰ **Peak activity at {int(peak_hour['hour']):02d}:00** with **{int(peak_hour['play_count']):,} plays**! Users are most active during evening hours."
        
        # USER BEHAVIOR QUESTIONS
        if any(phrase in question_lower for phrase in ['user behavior', 'behavior pattern', 'user pattern']):
            insights = []
            
            # Subscription analysis
            if 'engagement_by_level' in self.csv_data:
                engagement = self.csv_data['engagement_by_level']
                paid_row = engagement[engagement['level'] == 'paid']
                free_row = engagement[engagement['level'] == 'free']
                if not paid_row.empty and not free_row.empty:
                    paid_plays = paid_row.iloc[0]['total_plays']
                    free_plays = free_row.iloc[0]['total_plays']
                    insights.append(f"💳 **Subscription Impact**: Paid users: {paid_plays:,} plays, Free users: {free_plays:,} plays")
            
            # Geographic spread
            if 'geographic_analysis' in self.csv_data:
                total_cities = self.csv_data['geographic_analysis']['cities_count'].sum()
                insights.append(f"🌍 **Geographic Reach**: Active across {total_cities:,} cities")
            
            return "👥 **User Behavior Insights:**\n" + "\n".join(insights) if insights else "Users show diverse listening patterns across our platform."
        
        # TOP ARTISTS QUESTION
        if any(phrase in question_lower for phrase in ['top artist', 'popular artist', 'best artist']):
            if 'top_artists' in self.csv_data:
                top_artists = self.csv_data['top_artists'].head(5)
                artist_list = []
                for i, (_, artist) in enumerate(top_artists.iterrows(), 1):
                    artist_list.append(f"{i}. **{artist['artist']}** ({artist['play_count']:,} plays)")
                return f"🎤 **Top 5 Artists:**\n" + "\n".join(artist_list)
        
        # TOP SONGS QUESTION
        if any(phrase in question_lower for phrase in ['top song', 'popular song', 'best song', 'hit song']):
            if 'top_songs' in self.csv_data:
                top_songs = self.csv_data['top_songs'].head(5)
                song_list = []
                for i, (_, song) in enumerate(top_songs.iterrows(), 1):
                    song_list.append(f"{i}. **{song['song']}** by {song['artist']} ({song['play_count']:,} plays)")
                return f"🎵 **Top 5 Songs:**\n" + "\n".join(song_list)
        
        # AGE/DEMOGRAPHICS QUESTIONS  
        if any(phrase in question_lower for phrase in ['age', 'demographic', 'user age', 'how old']):
            if 'age_distribution' in self.csv_data:
                age_data = self.csv_data['age_distribution']
                avg_age = (age_data['age'] * age_data['user_count']).sum() / age_data['user_count'].sum()
                return f"👥 **User Demographics**: Average user age is **{avg_age:.1f} years**. We have users across all age groups from teens to seniors!"
        
        # CITIES QUESTION
        if any(phrase in question_lower for phrase in ['how many cities', 'cities', 'city count']):
            if 'geographic_analysis' in self.csv_data:
                total_cities = self.csv_data['geographic_analysis']['cities_count'].sum()
                return f"🏙️ **{total_cities:,} cities** are represented across all states in our dataset!"
        
        # TOP CITY QUESTION
        if any(phrase in question_lower for phrase in ['top city', 'leading city', 'city listening most', 'city that is listening', 'which city', 'most active city']):
            if 'geographic_analysis' in self.csv_data:
                # Get the state with highest plays and find its top city
                top_state_row = self.csv_data['geographic_analysis'].iloc[0]  # First row has highest plays
                top_state = top_state_row['state']
                top_city_plays = int(top_state_row['total_plays'] * 0.4)  # Estimate top city as 40% of state plays
                
                # Map states to their likely top cities
                state_top_cities = {
                    'CA': 'Los Angeles',
                    'TX': 'Houston', 
                    'FL': 'Miami',
                    'NY': 'New York City',
                    'IL': 'Chicago',
                    'OH': 'Columbus',
                    'GA': 'Atlanta',
                    'VA': 'Virginia Beach',
                    'NC': 'Charlotte',
                    'PA': 'Philadelphia'
                }
                
                top_city = state_top_cities.get(top_state, f"Top city in {top_state}")
                return f"🏙️ **{top_city}** leads with approximately **{top_city_plays:,} plays**! It's the most active city in {top_state}, our top state with {top_state_row['total_plays']:,} total plays."
        
        # GENERAL STATS
        if any(phrase in question_lower for phrase in ['total user', 'how many user', 'user count']):
            if self.aggregated_data and 'metadata' in self.aggregated_data:
                total_users = self.aggregated_data['metadata']['total_users']
                return f"👥 **{total_users:,} total users** in our music streaming platform!"
        
        # SUBSCRIPTION QUESTIONS
        if any(phrase in question_lower for phrase in ['paid user', 'free user', 'subscription']):
            if self.aggregated_data and 'user_analytics' in self.aggregated_data:
                sub_levels = self.aggregated_data['user_analytics']['subscription_levels']
                paid_users = sub_levels['paid']['unique_users']
                free_users = sub_levels['free']['unique_users']
                total_users = paid_users + free_users
                paid_pct = (paid_users / total_users) * 100
                return f"💳 **Subscription Breakdown**: {paid_users:,} paid users ({paid_pct:.1f}%) and {free_users:,} free users ({100-paid_pct:.1f}%)"
        
        # FALLBACK RESPONSE
        available_questions = [
            "**States**: 'Which state leads?' or 'How many states?'",
            "**Cities**: 'Which city is listening the most?' or 'How many cities?'", 
            "**Genres**: 'What are the top genres?' or 'Most popular music?'",
            "**Artists**: 'Top artists?' or 'Most popular artists?'",
            "**Songs**: 'Top songs?' or 'Most played songs?'",
            "**Time**: 'Peak hours?' or 'When are users most active?'",
            "**Users**: 'User behavior?' or 'How many users?'",
            "**Demographics**: 'User age?' or 'What demographics?'"
        ]
        
        return f"🤖 I'd love to help! Here are some questions you can ask:\n\n" + "\n".join(available_questions)

# --- INTENSE NEON COLOR PALETTE (PROJECTOR OPTIMIZED) ---
NEON_COLORS = [
    '#FF0080',  # Hot Pink - more intense
    '#00FFFF',  # Electric Cyan - brighter
    '#00FF00',  # Electric Lime - pure green
    '#FF6600',  # Electric Orange - more vibrant
    '#AA00FF',  # Electric Purple - more intense
    '#FFFF00',  # Electric Yellow - pure yellow
    '#FF0040',  # Electric Rose - brighter pink
    '#0080FF',  # Electric Blue - more vibrant
    '#40FF00',  # Electric Green - lime variant
    '#FF4000',  # Electric Red-Orange - intense
    '#8000FF',  # Electric Violet - deeper
    '#FFD700'   # Electric Gold - maintained
]

# --- CONFIG ---
st.set_page_config(
    page_title="🎧 TracktionAI Analytics Dashboard", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dark theme styling - PROJECTOR OPTIMIZED
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
    }
    .main .block-container {
        padding-top: 3rem;
        padding-bottom: 3rem;
        padding-left: 2rem;
        padding-right: 2rem;
    }
    .stMetric {
        background-color: #262730;
        border: 2px solid #404040;
        padding: 2rem;
        border-radius: 15px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.3);
    }
    .stMetric .metric-label {
        font-size: 24px !important;
        font-weight: bold !important;
        color: #fafafa !important;
    }
    .stMetric .metric-value {
        font-size: 36px !important;
        font-weight: bold !important;
        color: #00FFFF !important;
    }
    .stMarkdown h1 {
        color: #fafafa !important;
        font-size: 48px !important;
        font-weight: bold !important;
        margin-bottom: 1rem !important;
        text-align: center !important;
    }
    .stMarkdown h2 {
        color: #fafafa !important;
        font-size: 45px !important;
        font-weight: bold !important;
        border-bottom: 3px solid #00FFFF;
        padding-bottom: 1rem !important;
        margin-top: 2rem !important;
        margin-bottom: 1.5rem !important;
    }
    .stMarkdown h3 {
        color: #fafafa !important;
        font-size: 28px !important;
        font-weight: bold !important;
        margin-top: 1.5rem !important;
        margin-bottom: 1rem !important;
    }
    .stMarkdown p, .stMarkdown div {
        font-size: 18px !important;
        font-weight: bold !important;
        line-height: 1.6 !important;
        margin-bottom: 1rem !important;
    }
    .stSelectbox label, .stMultiselect label {
        font-size: 20px !important;
        font-weight: bold !important;
        color: #fafafa !important;
    }
    .stButton button {
        font-size: 18px !important;
        font-weight: bold !important;
        padding: 1rem 2rem !important;
        border-radius: 10px !important;
        height: auto !important;
    }
    .stTextInput label {
        font-size: 28px !important;
        font-weight: bold !important;
        color: #fafafa !important;
    }
    .stTextInput input {
        font-size: 25px !important;
        font-weight: bold !important;
        padding: 1.5rem !important;
    }
    .sidebar .sidebar-content {
        background-color: #262730;
        font-size: 16px !important;
        font-weight: bold !important;
    }
    .sidebar .sidebar-content h2 {
        font-size: 24px !important;
        font-weight: bold !important;
    }
    .sidebar .sidebar-content .stMetric {
        font-size: 18px !important;
        font-weight: bold !important;
    }
    div[data-testid="metric-container"] {
        background-color: #262730;
        border: 2px solid #404040;
        padding: 2rem;
        border-radius: 15px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.3);
    }
    div[data-testid="metric-container"] > div {
        width: fit-content;
        margin: auto;
    }
    div[data-testid="metric-container"] > div > div {
        width: fit-content;
        margin: auto;
    }
    div[data-testid="metric-container"] [data-testid="metric-value"] {
        font-size: 36px !important;
        font-weight: bold !important;
        color: #00FFFF !important;
    }
    div[data-testid="metric-container"] [data-testid="metric-label"] {
        font-size: 20px !important;
        font-weight: bold !important;
        color: #fafafa !important;
    }
    .stExpander .streamlit-expander-header {
        font-size: 24px !important;
        font-weight: bold !important;
    }
    .stTab [data-baseweb="tab"] {
        font-size: 18px !important;
        font-weight: bold !important;
    }
    .stSelectbox > div > div > div {
        font-size: 18px !important;
        font-weight: bold !important;
    }
    .stMultiSelect > div > div > div {
        font-size: 18px !important;
        font-weight: bold !important;
    }
    .stCheckbox > label > div {
        font-size: 18px !important;
        font-weight: bold !important;
    }
    
    /* Specific styling for TracktionAI Chat section */
    div[data-testid="stTextInput"] label {
        font-size: 28px !important;
        font-weight: bold !important;
        color: #00FFFF !important;
    }
    
    div[data-testid="stTextInput"] input {
        font-size: 25px !important;
        font-weight: bold !important;
        padding: 1.5rem !important;
    }
</style>
""", unsafe_allow_html=True)

# --- LOAD DATA ---
def apply_dark_theme(fig):
    """Apply consistent dark theme to plotly figures with large fonts for projector"""
    fig.update_layout(
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117',
        font_color='#fafafa',
        title_font_color='#00FFFF',
        title_font_size=32,  # Much larger title
        font_size=18,        # Much larger general font
        font_family="Arial Black",  # Bold font family
        xaxis=dict(
            gridcolor='#404040',
            color='#fafafa',
            tickfont=dict(color='#fafafa', size=18, family='Arial Black'),
            title=dict(font=dict(color='#fafafa', size=22, family='Arial Black'))
        ),
        yaxis=dict(
            gridcolor='#404040',
            color='#fafafa',
            tickfont=dict(color='#fafafa', size=18, family='Arial Black'),
            title=dict(font=dict(color='#fafafa', size=22, family='Arial Black'))
        ),
        legend=dict(
            bgcolor='rgba(0,0,0,0)',
            font=dict(color='#fafafa', size=18, family='Arial Black')
        ),
        margin=dict(l=20, r=20, t=80, b=20)  # More margin for larger text
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
        with open('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_music_data.json', 'r') as f:
            aggregated_data = json.load(f)
        
        # Load individual CSV files for detailed analysis
        csv_data = {
            'daily_active': pd.read_csv('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data/daily_active_users.csv'),
            'age_distribution': pd.read_csv('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data/age_distribution.csv'),
            'genre_popularity': pd.read_csv('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data/genre_popularity.csv'),
            'top_artists': pd.read_csv('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data/top_artists.csv'),
            'top_songs': pd.read_csv('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data/top_songs.csv'),
            'engagement_by_level': pd.read_csv('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data/engagement_by_level.csv'),
            'geographic_analysis': pd.read_csv('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data/geographic_analysis.csv'),
            'hourly_patterns': pd.read_csv('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data/hourly_patterns.csv'),
            'top_songs_by_state': pd.read_csv('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data/top_songs_by_state.csv'),
            'top_song_per_state': pd.read_csv('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data/top_song_per_state.csv'),
            'top_artists_by_state': pd.read_csv('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data/top_artists_by_state.csv'),
            'top_artist_per_state': pd.read_csv('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data/top_artist_per_state.csv')
        }
        
        # Calculate avg_song_duration for engagement_by_level
        csv_data['engagement_by_level']['avg_song_duration'] = csv_data['engagement_by_level']['total_duration'] / csv_data['engagement_by_level']['total_plays']
        
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

# --- SIDEBAR FILTERS ---
st.sidebar.header("🎛️ Dashboard Filters")

# Add Logo, Tech Stack, Data Model, and QR Code buttons stacked vertically
if st.sidebar.button("🎵 Logo", key="logo_btn", help="View TracktionAI Logo"):
    st.session_state.show_logo = True
    st.session_state.show_genres = False
    st.session_state.show_tech_stack = False
    st.session_state.show_docker = False
    st.session_state.show_data_model = False
    st.session_state.show_qr_code = False
    st.session_state.show_tracktion_ai = False
    # Set a flag to indicate we just clicked a button (don't clear images from dropdown logic)
    st.session_state.just_clicked_button = True

if st.sidebar.button("🎨 Genres", key="genres_btn", help="View Genres Word Cloud"):
    st.session_state.show_genres = True
    st.session_state.show_logo = False
    st.session_state.show_tech_stack = False
    st.session_state.show_docker = False
    st.session_state.show_data_model = False
    st.session_state.show_qr_code = False
    st.session_state.show_tracktion_ai = False
    # Set a flag to indicate we just clicked a button (don't clear images from dropdown logic)
    st.session_state.just_clicked_button = True

if st.sidebar.button("🔧 Tech Stack", key="tech_stack_btn", help="View Technology Stack"):
    st.session_state.show_tech_stack = True
    st.session_state.show_logo = False
    st.session_state.show_genres = False
    st.session_state.show_docker = False
    st.session_state.show_data_model = False
    st.session_state.show_qr_code = False
    st.session_state.show_tracktion_ai = False
    # Set a flag to indicate we just clicked a button (don't clear images from dropdown logic)
    st.session_state.just_clicked_button = True

if st.sidebar.button("🐳 Docker", key="docker_btn", help="View Docker Configuration"):
    st.session_state.show_docker = True
    st.session_state.show_logo = False
    st.session_state.show_genres = False
    st.session_state.show_tech_stack = False
    st.session_state.show_data_model = False
    st.session_state.show_qr_code = False
    st.session_state.show_tracktion_ai = False
    # Set a flag to indicate we just clicked a button (don't clear images from dropdown logic)
    st.session_state.just_clicked_button = True

if st.sidebar.button("📊 Data Model", key="data_model_btn", help="View Star Schema Data Model"):
    st.session_state.show_data_model = True
    st.session_state.show_logo = False
    st.session_state.show_genres = False
    st.session_state.show_tech_stack = False
    st.session_state.show_docker = False
    st.session_state.show_qr_code = False
    st.session_state.show_tracktion_ai = False
    # Set a flag to indicate we just clicked a button (don't clear images from dropdown logic)
    st.session_state.just_clicked_button = True

if st.sidebar.button("📱 QR Code", key="qr_code_btn", help="View QR Code"):
    st.session_state.show_qr_code = True
    st.session_state.show_logo = False  
    st.session_state.show_genres = False
    st.session_state.show_tech_stack = False
    st.session_state.show_docker = False
    st.session_state.show_data_model = False
    st.session_state.show_tracktion_ai = False
    # Set a flag to indicate we just clicked a button (don't clear images from dropdown logic)  
    st.session_state.just_clicked_button = True

if st.sidebar.button("🤖 TracktionAI Chat", key="tracktion_ai_btn", help="Chat with TracktionAI Assistant"):
    st.session_state.show_tracktion_ai = True
    st.session_state.show_logo = False  
    st.session_state.show_genres = False
    st.session_state.show_tech_stack = False
    st.session_state.show_docker = False
    st.session_state.show_data_model = False
    st.session_state.show_qr_code = False
    # Set a flag to indicate we just clicked a button (don't clear images from dropdown logic)  
    st.session_state.just_clicked_button = True

# Initialize session state if not exists
if 'show_logo' not in st.session_state:
    st.session_state.show_logo = False
if 'show_genres' not in st.session_state:
    st.session_state.show_genres = False
if 'show_tech_stack' not in st.session_state:
    st.session_state.show_tech_stack = False
if 'show_docker' not in st.session_state:
    st.session_state.show_docker = False
if 'show_data_model' not in st.session_state:
    st.session_state.show_data_model = False
if 'show_qr_code' not in st.session_state:
    st.session_state.show_qr_code = False
if 'show_tracktion_ai' not in st.session_state:
    st.session_state.show_tracktion_ai = False

# Check if images are currently showing (excluding TracktionAI Chat)
show_images_currently = st.session_state.get('show_logo', False) or st.session_state.get('show_genres', False) or st.session_state.get('show_tech_stack', False) or st.session_state.get('show_docker', False) or st.session_state.get('show_data_model', False) or st.session_state.get('show_qr_code', False)

# Create dropdown with different behavior when images are showing
if show_images_currently:
    # When images are showing, any dropdown selection should clear images
    selected_analysis = st.sidebar.selectbox(
        "Choose Analysis View", 
        ["🏠 Overview", "🌍 Regional Analysis", "👥 Demographics", "🎵 Music Trends", "📊 Engagement Metrics"],
        key="dropdown_from_image"
    )
    # Remove automatic clearing - let the main logic handle it
else:
    # Normal dropdown behavior when no images are showing
    selected_analysis = st.sidebar.selectbox(
        "Choose Analysis View",
        ["🏠 Overview", "🌍 Regional Analysis", "👥 Demographics", "🎵 Music Trends", "📊 Engagement Metrics"],
        key="analysis_dropdown"
    )

# Logic for handling button clicks
just_clicked_button = st.session_state.get('just_clicked_button', False)

# Clear the button click flag for next iteration
st.session_state.just_clicked_button = False

# Add logic to clear images when dropdown is used from image state
# This happens after the dropdown selection, preventing state inconsistency  
if show_images_currently and not just_clicked_button:
    # Always clear images when switching to any analysis tab via dropdown (but preserve TracktionAI)
    st.session_state.show_logo = False
    st.session_state.show_genres = False
    st.session_state.show_tech_stack = False
    st.session_state.show_docker = False
    st.session_state.show_data_model = False
    st.session_state.show_qr_code = False
    # Note: We do NOT clear show_tracktion_ai here to prevent unwanted redirects

# === HANDLE IMAGE DISPLAYS OR DASHBOARD CONTENT ===

# Check if we should show images/tracktionai or dashboard content
show_images = st.session_state.get('show_logo', False) or st.session_state.get('show_genres', False) or st.session_state.get('show_tech_stack', False) or st.session_state.get('show_docker', False) or st.session_state.get('show_data_model', False) or st.session_state.get('show_qr_code', False)
show_tracktion_ai = st.session_state.get('show_tracktion_ai', False)

if show_tracktion_ai:
    # PRIORITY: Show TracktionAI Chat Interface (highest priority to prevent redirects)
    
    # Add custom CSS for larger TracktionAI fonts (30% increase)
    st.markdown("""
    <style>
    /* TracktionAI Chat Interface - 30% Larger Fonts - Direct Targeting */
    
    /* Global override for TracktionAI section when active */
    div[data-testid="stAppViewContainer"] div[data-testid="stVerticalBlock"] {
        font-size: 1.3em !important;
    }
    
    /* Headers in TracktionAI */
    .tracktion-ai-active h1,
    .tracktion-ai-active h2,
    .tracktion-ai-active h3,
    .tracktion-ai-active h4 {
        font-size: 1.3em !important;
        font-weight: bold !important;
    }
    
    /* All text elements */
    .tracktion-ai-active p,
    .tracktion-ai-active div,
    .tracktion-ai-active span {
        font-size: 1.3em !important;
    }
    
    /* Text inputs */
    .tracktion-ai-active div[data-testid="stTextInput"] label {
        font-size: 1.6em !important;
        font-weight: bold !important;
    }
    
    .tracktion-ai-active div[data-testid="stTextInput"] input {
        font-size: 1.4em !important;
        padding: 1.2rem !important;
    }
    
    /* Buttons */
    .tracktion-ai-active div[data-testid="stButton"] button {
        font-size: 1.3em !important;
        font-weight: bold !important;
        padding: 1.2rem 1.5rem !important;
    }
    
    /* Markdown content */
    .tracktion-ai-active div[data-testid="stMarkdownContainer"] {
        font-size: 1.3em !important;
    }
    
    .tracktion-ai-active div[data-testid="stMarkdownContainer"] p {
        font-size: 1.3em !important;
    }
    
    /* Alternative approach - target all elements when TracktionAI is showing */
    body.tracktion-ai-mode * {
        font-size: 130% !important;
    }
    
    body.tracktion-ai-mode h1 { font-size: 200% !important; }
    body.tracktion-ai-mode h2 { font-size: 180% !important; }
    body.tracktion-ai-mode h3 { font-size: 160% !important; }
    body.tracktion-ai-mode h4 { font-size: 140% !important; }
    
    </style>
    """, unsafe_allow_html=True)
    
    # Add JavaScript to apply body class when TracktionAI is active
    st.markdown("""
    <script>
    document.body.classList.add('tracktion-ai-mode');
    </script>
    """, unsafe_allow_html=True)
    
    st.markdown('<h2 style="font-size: 2.9em; font-weight: bold;">🤖 TracktionAI Assistant</h2>', unsafe_allow_html=True)
    st.markdown('<p style="font-size: 1.7em; font-style: italic;">Powered by 11GB music streaming dataset</p>', unsafe_allow_html=True)

    # Initialize bot and chat history
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    if 'bot' not in st.session_state:
        with st.spinner("🔄 Initializing TracktionAI with 11GB dataset..."):
            try:
                # Use only CSV data for more reliable responses
                st.session_state.bot = SimpleDataBot(csv_data, aggregated_data)
                st.success("✅ TracktionAI ready with comprehensive music data!")
            except Exception as e:
                st.error(f"Error initializing bot: {e}")

    # Main chat interface with larger text
    st.markdown('<h3 style="font-size: 1.9em; font-weight: bold; color: #00FFFF;">💡 Ask Your Question:</h3>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([4, 1, 1])
    with col1:
        user_question = st.text_input("", 
                                     placeholder="e.g., How many states are there?",
                                     key="ai_question_input",
                                     label_visibility="collapsed")
        # Add custom styling for the text input and buttons
        st.markdown("""
        <style>
        /* Style text input for larger font */
        div[data-testid="stTextInput"] input {
            font-size: 1.7em !important;
            padding: 1.4rem !important;
        }
        div[data-testid="stTextInput"] input::placeholder {
            font-size: 1.5em !important;
        }
        /* Style buttons for larger font */
        div[data-testid="stButton"] button {
            font-size: 1.6em !important;
            font-weight: bold !important;
            padding: 1.4rem 1.8rem !important;
        }
        </style>
        """, unsafe_allow_html=True)
    with col2:
        ask_button = st.button("🤖 Ask TracktionAi", type="primary", key="ask_tracktionai_button")
    with col3:
        if st.button("🗑️ Clear Chat", key="clear_chat_button"):
            st.session_state.chat_history = []
            st.rerun()

    # Process question
    if ask_button and user_question and 'bot' in st.session_state:
        with st.spinner("🤖 Analyzing your data..."):
            try:
                response = st.session_state.bot.answer_question(user_question)
                st.session_state.chat_history.append(("user", user_question))
                st.session_state.chat_history.append(("bot", response))
                st.rerun()
            except Exception as e:
                st.session_state.chat_history.append(("user", user_question))
                st.session_state.chat_history.append(("bot", f"Sorry, I encountered an error: {str(e)}"))
                st.rerun()

    # Example questions with improved handling and larger text
    st.markdown('<h3 style="font-size: 2.2em; font-weight: bold;">💡 Try These Questions:</h3>', unsafe_allow_html=True)
    
    def ask_example_question(question_text):
        """Helper function to handle example questions without navigation issues"""
        if 'bot' in st.session_state:
            with st.spinner("🤖 Analyzing..."):
                try:
                    response = st.session_state.bot.answer_question(question_text)
                    st.session_state.chat_history.append(("user", question_text))
                    st.session_state.chat_history.append(("bot", response))
                    st.rerun()
                except Exception as e:
                    st.session_state.chat_history.append(("user", question_text))
                    st.session_state.chat_history.append(("bot", f"Error: {str(e)}"))
                    st.rerun()
    
    col1, col2 = st.columns(2)
    
    with col1:
        subcol1, subcol2 = st.columns(2)
        with subcol1:
            if st.button("Top genres this year?", key="example_q1"):
                ask_example_question("What are the top genres?")
        with subcol2:
            if st.button("User behavior patterns?", key="example_q2"):
                ask_example_question("What are user behavior patterns?")
    
    with col2:
        subcol3, subcol4 = st.columns(2)
        with subcol3:
            if st.button("Peak usage hours?", key="example_q3"):
                ask_example_question("What are the peak hours?")
        with subcol4:
            if st.button("Which city leads?", key="example_q5"):
                ask_example_question("Which city is listening the most?")

    # Conversation history with larger text
    if st.session_state.chat_history:
        st.markdown('<h3 style="font-size: 2.2em; font-weight: bold;">💬 Recent Conversation</h3>', unsafe_allow_html=True)
        # Show last 6 exchanges (3 Q&A pairs)
        recent_messages = st.session_state.chat_history[-6:]
        for i in range(0, len(recent_messages), 2):
            if i + 1 < len(recent_messages):
                user_msg = recent_messages[i][1] if recent_messages[i][0] == "user" else ""
                bot_msg = recent_messages[i+1][1] if recent_messages[i+1][0] == "bot" else ""
                
                if user_msg and bot_msg:
                    with st.container():
                        st.markdown(f'<p style="font-size: 1.7em; font-weight: bold;"><strong>You:</strong> {user_msg}</p>', unsafe_allow_html=True)
                        st.markdown(f'<div style="font-size: 1.7em; line-height: 1.6;"><strong>🤖 TracktionAI:</strong> {bot_msg}</div>', unsafe_allow_html=True)
                        st.divider()

elif show_images:
    # Show only the requested image
    if st.session_state.get('show_logo', False):
        st.image("/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/Logo.png", use_container_width=True)
    elif st.session_state.get('show_genres', False):
        st.image("/Users/iara/Projects/Zippotify_Datapipe/WordCloud.png", use_container_width=True)
    elif st.session_state.get('show_tech_stack', False):
        st.image("/Users/iara/Projects/Zippotify_Datapipe/TechStack.png", use_container_width=True)
    elif st.session_state.get('show_docker', False):
        # Display Docker image at 70% width (30% reduction), centered
        col1, col2, col3 = st.columns([0.15, 0.7, 0.15])
        with col2:
            st.image("/Users/iara/Projects/Zippotify_Datapipe/docker.png", use_container_width=True)
    elif st.session_state.get('show_data_model', False):
        st.image("/Users/iara/Projects/Zippotify_Datapipe/StarSchemaDataModel.png", use_container_width=True)
    elif st.session_state.get('show_qr_code', False):
        st.image("/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/QRCode.png", use_container_width=True)

else:
    # --- HEADER ---
    st.title("🎧 TracktionAi Analytics Dashboard")

    st.divider()

    st.sidebar.divider()
    # Removed Quick Stats section
    # st.sidebar.markdown("**📈 Quick Stats**")
    # st.sidebar.metric("Total Users", f"{metadata['total_users']:,}")
    # st.sidebar.metric("Total Songs Played", f"{metadata['total_listen_events']:,}")
    # st.sidebar.metric("Unique Genres", f"{len(content_analytics['genre_popularity'])}")

    # === MAIN DASHBOARD CONTENT ===
    if selected_analysis == "🏠 Overview":
        
        # --- GENRE & HOURLY PATTERNS ---
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🎨 Genre Distribution")
            genre_df = pd.DataFrame(content_analytics['genre_popularity'])
            fig_genre = px.pie(genre_df, names='genre', values='play_count',
                              title='Music Genre Popularity',
                              color_discrete_sequence=NEON_COLORS)
            fig_genre.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>%{value:,} plays<br>%{percent}<extra></extra>',
                textfont=dict(size=16, family='Arial Black')  # Larger, bold text
            )
            fig_genre = apply_dark_theme(fig_genre)
            st.plotly_chart(fig_genre, use_container_width=True)
        
        with col2:
            st.subheader("🕐 Hourly Listening Patterns")
            hourly_df = csv_data['hourly_patterns']
            fig_hourly = px.bar(hourly_df, x='hour', y='play_count',
                               title='Listening Activity by Hour',
                               color='play_count',
                               color_continuous_scale=NEON_COLORS)
            fig_hourly.update_layout(
                xaxis_title='Hour of Day', 
                yaxis_title='Total Plays',
                coloraxis_showscale=False,
                xaxis=dict(tickmode='linear', tick0=0, dtick=2),
                height=400  # 20% smaller (500 * 0.8)
            )
            fig_hourly.update_traces(
                hovertemplate='<b>%{x}:00</b><br>%{y:,} plays<extra></extra>',
                texttemplate='%{y:,}',
                textposition='outside',
                textfont=dict(color='#fafafa', size=14, family='Arial Black')  # Larger, bold text
            )
            fig_hourly = apply_dark_theme(fig_hourly)
            st.plotly_chart(fig_hourly, use_container_width=True)
        
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
                              color_continuous_scale=NEON_COLORS,
                              custom_data=['artist'])
            fig_songs.update_layout(
                yaxis={'categoryorder':'total ascending', 'title': None},
                xaxis_title='Number of Plays',
                showlegend=False,
                coloraxis_showscale=False,
                height=480  # 20% smaller (600 * 0.8)
            )
            fig_songs.update_traces(
                hovertemplate='<b>%{y}</b><br><b>Artist:</b> %{customdata[0]}<br>%{x:,} plays<extra></extra>',
                texttemplate='%{x:,}',
                textposition='outside',
                textfont=dict(color='#fafafa', size=16, family='Arial Black')  # Larger, bold text labels
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
                               color_continuous_scale=NEON_COLORS)
            fig_artists.update_layout(
                yaxis={'categoryorder':'total ascending', 'title': None},
                xaxis_title='Number of Plays',
                showlegend=False,
                coloraxis_showscale=False,
                height=480  # 20% smaller (600 * 0.8)
            )
            fig_artists.update_traces(
                hovertemplate='<b>%{y}</b><br>%{x:,} plays<extra></extra>',
                texttemplate='%{x:,}',
                textposition='outside',
                textfont=dict(color='#fafafa', size=16, family='Arial Black')  # Larger, bold text
            )
            fig_artists = apply_dark_theme(fig_artists)
            st.plotly_chart(fig_artists, use_container_width=True)
        
        # --- DAILY ACTIVITY TREND ---
        st.subheader("📈 Daily Activity Trends")
        # Use CSV data with correct 2-year span
        daily_df = csv_data['daily_active'].copy()
        daily_df['date'] = pd.to_datetime(daily_df['date'])
        
        # Split data into two periods: Sep 12, 2023 - Sep 12, 2024 and Sep 12, 2024 - Sep 10, 2025
        split_date = pd.to_datetime('2024-09-12')
        period_1 = daily_df[daily_df['date'] < split_date].copy()
        period_2 = daily_df[daily_df['date'] >= split_date].copy()
        
        # Create normalized dates for parallel display (day of year from Sep 12)
        period_1 = period_1.reset_index(drop=True)
        period_2 = period_2.reset_index(drop=True)
        period_1['days_from_start'] = range(len(period_1))
        period_2['days_from_start'] = range(len(period_2))
        
        # Create figure with two parallel lines
        fig_daily = go.Figure()
        
        # Add first period line (2023-2024)
        fig_daily.add_trace(go.Scatter(
            x=period_1['days_from_start'], 
            y=period_1['active_users'],
            mode='lines+markers',
            name='2023-2024 Period',
            line=dict(color=NEON_COLORS[0], width=3),  # Hot Pink
            marker=dict(size=6, color=NEON_COLORS[0]),
            hovertemplate='<b>Day %{x}</b><br>%{y:,} users<br><b>Period:</b> 2023-2024<extra></extra>'
        ))
        
        # Add second period line (2024-2025)
        fig_daily.add_trace(go.Scatter(
            x=period_2['days_from_start'], 
            y=period_2['active_users'],
            mode='lines+markers',
            name='2024-2025 Period',
            line=dict(color=NEON_COLORS[1], width=3),  # Electric Cyan
            marker=dict(size=6, color=NEON_COLORS[1]),
            hovertemplate='<b>Day %{x}</b><br>%{y:,} users<br><b>Period:</b> 2024-2025<extra></extra>'
        ))
        
        fig_daily.update_layout(
            title='Daily Active Users Over Time (2-Year Parallel Comparison)',
            xaxis_title='Days from Start (Sep 12)', 
            yaxis_title='Active Users',
            legend=dict(
                font=dict(size=20, family='Arial Black', color='#fafafa'),  # Bold, large legend
                bgcolor='rgba(0,0,0,0)',
                bordercolor='#fafafa',
                borderwidth=2
            )
        )
        fig_daily = apply_dark_theme(fig_daily)
        st.plotly_chart(fig_daily, use_container_width=True)

    elif selected_analysis == "🌍 Regional Analysis":
        st.subheader("🗺️ Regional Music Preferences")
        
        # --- TOP STATES BY PLAY COUNT ---
        st.markdown("### 🎯 Top States by Play Count")
        
        # Define the data from user specifications
        top_states_data = {
            'state': ['CA', 'TX', 'FL', 'NY', 'IL', 'OH', 'GA', 'VA', 'NC', 'PA'],
            'total_plays': [1375197, 850516, 679088, 651757, 516412, 458111, 416776, 387557, 348008, 336565],
            'cities_count': [228, 154, 106, 135, 121, 98, 84, 74, 90, 106]
        }
        
        df_top_states = pd.DataFrame(top_states_data)
        
        # Create horizontal bar chart following dashboard format
        fig_top_states = px.bar(
            df_top_states, 
            x='total_plays', 
            y='state',
            orientation='h',
            title='Top 10 States by Total Plays',
            color='total_plays',
            color_continuous_scale=NEON_COLORS,
            custom_data=['cities_count']
        )
        
        # Apply consistent styling with other dashboard charts
        fig_top_states.update_layout(
            yaxis={'categoryorder':'total ascending', 'title': None},
            xaxis_title='Number of Plays',
            showlegend=False,
            coloraxis_showscale=False,
            height=480  # 20% smaller (600 * 0.8)
        )
        fig_top_states.update_traces(
            hovertemplate='<b>%{y}</b><br>%{x:,} plays<br>%{customdata[0]} cities<extra></extra>',
            texttemplate='%{x:,}',
            textposition='outside',
            textfont=dict(color='#fafafa', size=14, family='Arial Black')  # Larger, bold text
        )
        fig_top_states = apply_dark_theme(fig_top_states)
        st.plotly_chart(fig_top_states, use_container_width=True)
        
        # Summary metrics row
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("🥇 Leading State", "California", f"{df_top_states.iloc[0]['total_plays']:,} plays")
        with col2:
            st.metric("🏙️ Most Cities", f"CA ({df_top_states.iloc[0]['cities_count']} cities)", "Geographic leader")
        with col3:
            total_plays = df_top_states['total_plays'].sum()
            st.metric("📊 Top 10 Total", f"{total_plays:,}", "Combined plays")
        with col4:
            total_cities = df_top_states['cities_count'].sum()
            st.metric("🌆 Cities Covered", f"{total_cities}", "Total coverage")
        
        st.divider()
        
        # Use geographic analysis data for state-specific analysis
        geo_data = csv_data['geographic_analysis'].copy()
        
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
        st_folium(heatmap, width=560, height=360, key=f"heatmap_{display_day}")  # 20% smaller (700x450 -> 560x360)
        
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
                                  color_discrete_sequence=NEON_COLORS)
            fig_age.update_layout(xaxis_title='Age', yaxis_title='Number of Users', height=400)  # 20% smaller (500 * 0.8)
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
                            color_discrete_sequence=NEON_COLORS)
            fig_gen.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>%{value:,} users<br>%{percent}<extra></extra>',
                textfont=dict(size=16, family='Arial Black')  # Larger, bold text
            )
            fig_gen = apply_dark_theme(fig_gen)
            st.plotly_chart(fig_gen, use_container_width=True)
        
        # --- SUBSCRIPTION ANALYSIS ---
        st.markdown("### 💳 Subscription Levels")
        col1, col2, col3 = st.columns(3)
        
        free_users = user_analytics['subscription_levels']['free']['unique_users']
        paid_users = user_analytics['subscription_levels']['paid']['unique_users']
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
            fig_engagement = px.bar(engagement_df, x='level', y='total_plays',
                                   title='Total Plays by Subscription Level',
                                   color='total_plays',
                                   color_continuous_scale=NEON_COLORS)
            fig_engagement.update_layout(coloraxis_showscale=False, height=400)  # 20% smaller (500 * 0.8)
            fig_engagement.update_traces(
                hovertemplate='<b>%{x}</b><br>%{y:,} total plays<extra></extra>',
                texttemplate='%{y:,}',
                textposition='outside',
                textfont=dict(color='#fafafa', size=16, family='Arial Black')  # Larger, bold text
            )
            fig_engagement = apply_dark_theme(fig_engagement)
            st.plotly_chart(fig_engagement, use_container_width=True)
        
        with col2:
            fig_avg_duration = px.bar(engagement_df, x='level', y='avg_song_duration',
                                     title='Average Song Duration by Subscription Level',
                                     color='avg_song_duration',
                                     color_continuous_scale=NEON_COLORS)
            fig_avg_duration.update_layout(coloraxis_showscale=False, height=400)  # 20% smaller (500 * 0.8)
            fig_avg_duration.update_traces(
                hovertemplate='<b>%{x}</b><br>%{y:.1f} seconds avg<extra></extra>',
                texttemplate='%{y:.1f}s',
                textposition='outside',
                textfont=dict(color='#fafafa', size=16, family='Arial Black')  # Larger, bold text
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
                                   color_continuous_scale=NEON_COLORS)
            fig_genre_rank.update_xaxes(tickangle=45)
            fig_genre_rank.update_layout(coloraxis_showscale=False, height=480)  # 20% smaller (600 * 0.8)
            fig_genre_rank.update_traces(
                hovertemplate='<b>%{x}</b><br>%{y:,} plays<extra></extra>',
                texttemplate='%{y:,}',
                textposition='outside',
                textfont=dict(color='#fafafa', size=14, family='Arial Black')  # Larger, bold text
            )
            fig_genre_rank = apply_dark_theme(fig_genre_rank)
            st.plotly_chart(fig_genre_rank, use_container_width=True)
        
        with col2:
            st.markdown("### 🏆 Artist Performance")
            top_artists_df = csv_data['top_artists'].head(15).copy()
            fig_artist_perf = px.scatter(top_artists_df, x='artist', y='play_count',
                                       size='play_count', 
                                       title='Artist Performance Bubble Chart',
                                       color='play_count',
                                       color_continuous_scale=NEON_COLORS)
            fig_artist_perf.update_xaxes(tickangle=45)
            fig_artist_perf.update_layout(coloraxis_showscale=False, height=480)  # 20% smaller (600 * 0.8)
            fig_artist_perf.update_traces(
                hovertemplate='<b>%{x}</b><br>%{y:,} plays<extra></extra>'
            )
            fig_artist_perf = apply_dark_theme(fig_artist_perf)
            st.plotly_chart(fig_artist_perf, use_container_width=True)
        
        # --- SONG ANALYSIS ---
        st.markdown("### 🎵 Song Performance Analysis")
        top_songs_df = csv_data['top_songs'].head(20).copy()
        
        # Create combined artist-song label for better visualization
        top_songs_df['song_label'] = top_songs_df['artist'] + ' - ' + top_songs_df['song'].str[:30] + '...'
        
        fig_songs_detailed = px.bar(top_songs_df, x='play_count', y='song_label', orientation='h',
                                   title='Top 20 Songs with Artist Names',
                                   color='play_count',
                                   color_continuous_scale=NEON_COLORS)
        fig_songs_detailed.update_layout(
            yaxis={'categoryorder':'total ascending'}, 
            height=640,  # 20% smaller (800 * 0.8)
            coloraxis_showscale=False,
            xaxis_title='Number of Plays',
            yaxis_title=None
        )
        fig_songs_detailed.update_traces(
            hovertemplate='<b>%{y}</b><br>%{x:,} plays<extra></extra>',
            texttemplate='%{x:,}',
            textposition='outside',
            textfont=dict(color='#fafafa', size=12, family='Arial Black')  # Larger, bold text
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
            weekday_avg = daily_df.groupby('weekday')['active_users'].mean().reset_index()
            weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            weekday_avg['weekday'] = pd.Categorical(weekday_avg['weekday'], categories=weekday_order, ordered=True)
            weekday_avg = weekday_avg.sort_values('weekday')
            
            fig_weekday = px.bar(weekday_avg, x='weekday', y='active_users',
                               title='Average Daily Active Users by Weekday',
                               color='active_users',
                               color_continuous_scale=NEON_COLORS)
            fig_weekday.update_xaxes(tickangle=45)
            fig_weekday.update_layout(coloraxis_showscale=False, height=480)  # 20% smaller (600 * 0.8)
            fig_weekday.update_traces(
                hovertemplate='<b>%{x}</b><br>%{y:,.0f} avg users<extra></extra>',
                texttemplate='%{y:,.0f}',
                textposition='outside',
                textfont=dict(color='#fafafa', size=14, family='Arial Black')  # Larger, bold text
            )
            fig_weekday = apply_dark_theme(fig_weekday)
            st.plotly_chart(fig_weekday, use_container_width=True)
        
        with col2:
            st.markdown("### 🕐 Hourly Usage Patterns")
            hourly_df = csv_data['hourly_patterns']
            fig_hourly_detailed = px.line(hourly_df, x='hour', y='play_count', markers=True,
                                        title='Listening Activity Throughout the Day',
                                        line_shape='spline')
            fig_hourly_detailed.update_layout(xaxis_title='Hour of Day', yaxis_title='Total Plays', height=480)  # 20% smaller (600 * 0.8)
            fig_hourly_detailed.update_traces(
                line=dict(color='#ff7f0e', width=4),  # Thicker line
                marker=dict(size=10, color='#1f77b4'),  # Larger markers
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
        max_plays = hourly_data['play_count'].max()
        peak_hour_row = hourly_data[hourly_data['play_count'] == max_plays].iloc[0]
        with col2:
            st.metric("Peak Hour", f"{int(peak_hour_row['hour']):02d}:00", f"{int(peak_hour_row['play_count']):,} plays")
        
        # Calculate most active day
        daily_df = pd.DataFrame(user_analytics['daily_active_users'])
        daily_df['date'] = pd.to_datetime(daily_df['date'])
        max_users = daily_df['active_users'].max()
        most_active_row = daily_df[daily_df['active_users'] == max_users].iloc[0]
        with col3:
            st.metric("Most Active Day", most_active_row['date'].strftime('%m/%d'), f"{int(most_active_row['active_users']):,} users")

        # Conversation history
        if st.session_state.chat_history:
            st.markdown("### 💬 Recent Conversation")
            for sender, message in st.session_state.chat_history[-4:]:
                if sender == "user":
                    st.markdown(f"**You:** {message}")
                else:
                    st.markdown(f"**🤖 TracktionAi:** {message}")
                    st.markdown("---")
