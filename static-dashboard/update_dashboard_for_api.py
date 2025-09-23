#!/usr/bin/env python3
"""
Script to update existing Streamlit dashboard to use FastAPI endpoints
Run this to convert your CSV-based dashboard to API-powered dashboard
"""

import re

def update_dashboard_for_api():
    """Update the existing dashboard.py to use API calls"""
    
    # Read existing dashboard
    with open('dashboard.py', 'r') as f:
        content = f.read()
    
    # Add API imports at the top
    api_imports = """import requests
import json
from typing import Optional
"""
    
    # Insert after existing imports
    content = content.replace('import json', f'{api_imports}import json')
    
    # Add API configuration
    api_config = """
# API Configuration
API_BASE_URL = "http://localhost:8000"  # FastAPI server URL

def fetch_api_data(endpoint: str) -> Optional[dict]:
    \"\"\"Fetch data from FastAPI endpoint with error handling\"\"\"
    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        st.error(f"❌ Cannot connect to FastAPI server at {API_BASE_URL}")
        st.info("Please ensure FastAPI is running: `uvicorn fast_api:app --reload`")
        return None
    except Exception as e:
        st.error(f"❌ Error fetching data from {endpoint}: {e}")
        return None

def test_api_connection() -> bool:
    \"\"\"Test if FastAPI server is accessible\"\"\"
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False

"""
    
    # Insert API functions after the apply_dark_theme function
    content = content.replace('def create_listening_heatmap', f'{api_config}def create_listening_heatmap')
    
    # Replace CSV loading with API calls
    api_load_function = '''@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_aggregated_data():
    """Load data from FastAPI endpoints or fallback to CSV"""
    
    # Test API connection first
    api_available = test_api_connection()
    
    if api_available:
        st.sidebar.success("🟢 Using Live API Data")
        
        # Load data from API endpoints
        aggregated_data = {
            'user_analytics': {
                'daily_active_users': fetch_api_data("/user_analytics/daily_active_users"),
                'age_distribution': fetch_api_data("/user_analytics/age_distribution")
            },
            'content_analytics': {
                'genre_popularity': fetch_api_data("/content_analytics/genre_popularity"),
                'top_songs': fetch_api_data("/content_analytics/top_songs"),
                'top_artists': fetch_api_data("/content_analytics/top_artists")
            },
            'engagement_analytics': {
                'by_subscription_level': fetch_api_data("/engagement_analytics/by_subscription_level"),
                'hourly_patterns': fetch_api_data("/engagement_analytics/hourly_patterns"),
                'geographic_distribution': fetch_api_data("/engagement_analytics/geographic_distribution")
            },
            'metadata': {
                'source': 'api',
                'generated_at': datetime.now().isoformat(),
                'total_users': len(fetch_api_data("/users") or []),
                'api_status': 'connected'
            }
        }
        
        # Convert API responses to expected format
        csv_data = {
            'daily_active': pd.DataFrame(aggregated_data['user_analytics']['daily_active_users'] or []),
            'age_distribution': pd.DataFrame(aggregated_data['user_analytics']['age_distribution'] or []),
            'genre_popularity': pd.DataFrame(aggregated_data['content_analytics']['genre_popularity'] or []),
            'top_artists': pd.DataFrame(aggregated_data['content_analytics']['top_artists'] or []),
            'top_songs': pd.DataFrame(aggregated_data['content_analytics']['top_songs'] or []),
            'engagement_by_level': pd.DataFrame(aggregated_data['engagement_analytics']['by_subscription_level'] or []),
            'hourly_patterns': pd.DataFrame(aggregated_data['engagement_analytics']['hourly_patterns'] or []),
            'geographic_analysis': pd.DataFrame(aggregated_data['engagement_analytics']['geographic_distribution'] or []),
            # Add state-specific data
            'top_artist_per_state': pd.DataFrame([
                {'state': state, 'artist': info['artist'], 'play_count': info['play_count']}
                for state, info in (fetch_api_data("/content_analytics/popular_artist_by_state") or {}).items()
            ])
        }
        
        return aggregated_data, csv_data
    
    else:
        # Fallback to original CSV loading
        st.sidebar.warning("🟡 Using Static CSV Data (API Unavailable)")
        try:
            # Original CSV loading logic
            with open('../aggregated_music_data.json', 'r') as f:
                aggregated_data = json.load(f)
                aggregated_data['metadata']['source'] = 'csv_fallback'
            
            csv_data = {
                'daily_active': pd.read_csv('aggregated_data/daily_active_users.csv', encoding='utf-8'),
                'age_distribution': pd.read_csv('aggregated_data/age_distribution.csv', encoding='utf-8'),
                # ... rest of CSV loading
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
            st.error(f"Error loading CSV fallback data: {e}")
            st.info("Please ensure you've run the ETL pipeline first: `python etl/aggregated_data.py`")
            return None, None'''
    
    # Replace the existing load_aggregated_data function
    content = re.sub(
        r'@st\.cache_data\ndef load_aggregated_data\(\):.*?return None, None',
        api_load_function,
        content,
        flags=re.DOTALL
    )
    
    # Write updated dashboard
    with open('api_enhanced_dashboard.py', 'w') as f:
        f.write(content)
    
    print("✅ Created api_enhanced_dashboard.py - your dashboard now supports both API and CSV!")
    print("🚀 To use: streamlit run api_enhanced_dashboard.py")

if __name__ == "__main__":
    update_dashboard_for_api()