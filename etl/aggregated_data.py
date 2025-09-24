import pandas as pd
import numpy as np
import json
from datetime import datetime
import random

# Set random seed for reproducible results
np.random.seed(42)
random.seed(42)

print("Loading data files...")

# Load JSON files from sample directory
auth = pd.read_json("data/sample/auth_events_head.jsonl", lines=True)
listen = pd.read_json("data/sample/listen_events_head.jsonl", lines=True)
page = pd.read_json("data/sample/page_view_events_head.jsonl", lines=True)
status = pd.read_json("data/sample/status_change_events_head.jsonl", lines=True)

print(f"Loaded {len(auth)} auth events, {len(listen)} listen events, {len(page)} page events, {len(status)} status events")

# Generate synthetic age data for users
def generate_age():
    """Generate realistic age distribution for music streaming users"""
    age_ranges = [
        (13, 28, 0.35),  # 35% Gen Z (born 1997-2012)
        (29, 44, 0.30),  # 30% Millennials (born 1981-1996)
        (45, 60, 0.20),  # 20% Gen X (born 1965-1980)
        (61, 79, 0.15)   # 15% Baby Boomers (born 1946-1964)
    ]
    
    range_choice = np.random.choice(len(age_ranges), p=[r[2] for r in age_ranges])
    min_age, max_age, _ = age_ranges[range_choice]
    return np.random.randint(min_age, max_age + 1)

# Generate synthetic genre data
def generate_genre_for_artist(artist_name):
    """Generate genres based on artist names with some logic"""
    genres = {
        'pop': ['Justin', 'Taylor', 'Ariana', 'Ed', 'Billie', 'Dua', 'The Weeknd'],
        'rock': ['Queen', 'Beatles', 'Led', 'Pink', 'Foo', 'Red', 'Green'],
        'hip-hop': ['Drake', 'Kendrick', 'Jay', 'Kanye', 'Eminem', 'Nas', 'Future'],
        'electronic': ['Skrillex', 'Calvin', 'David', 'Deadmau5', 'Daft', 'Tiësto', 'Skream'],
        'r&b': ['Beyoncé', 'Alicia', 'John', 'Bruno', 'The Weeknd', 'SZA'],
        'country': ['Luke', 'Keith', 'Carrie', 'Blake', 'Miranda', 'Chris'],
        'jazz': ['Miles', 'John', 'Ella', 'Louis', 'Duke', 'Charlie'],
        'classical': ['Mozart', 'Bach', 'Beethoven', 'Chopin', 'Ludovico'],
        'indie': ['Arctic', 'Vampire', 'Foster', 'Mumford', 'Of Monsters', 'Bon Iver'],
        'latin': ['Shakira', 'Manu', 'Jesse', 'Mana', 'Julieta'],
        'reggae': ['Bob', 'Jimmy', 'Damian', 'Stephen'],
        'folk': ['Bob Dylan', 'Joni', 'Neil', 'Fleetwood', 'Simon'],
        'funk': ['James', 'Parliament', 'Sly', 'George', 'Prince'],
        'alternative': ['Radiohead', 'Pearl', 'Nirvana', 'Stone', 'Alice']
    }
    
    # Default genre distribution
    all_genres = ['pop', 'rock', 'hip-hop', 'electronic', 'r&b', 'country', 'jazz', 
                  'classical', 'indie', 'latin', 'reggae', 'folk', 'funk', 'alternative']
    
    # Check if artist matches any genre keywords
    artist_lower = artist_name.lower()
    for genre, keywords in genres.items():
        for keyword in keywords:
            if keyword.lower() in artist_lower:
                return genre
    
    # If no match, assign based on weighted probability
    weights = [0.20, 0.15, 0.12, 0.10, 0.08, 0.06, 0.04, 0.03, 0.07, 0.05, 0.02, 0.03, 0.02, 0.03]
    return np.random.choice(all_genres, p=weights)

print("Processing and enriching data...")

# Add synthetic ages to auth data
unique_users = auth['userId'].unique()
user_ages = {user_id: generate_age() for user_id in unique_users}
auth['age'] = auth['userId'].map(user_ages)

# Add synthetic genres to listen data
listen['genre'] = listen['artist'].apply(generate_genre_for_artist)

# Convert timestamps to readable dates
for df in [auth, listen, page, status]:
    if 'ts' in df.columns:
        df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
        df['date'] = df['datetime'].dt.date
        df['hour'] = df['datetime'].dt.hour

# Create comprehensive aggregated data
print("Creating aggregated analytics...")

# Daily active users
daily_active = listen.groupby('date')['userId'].nunique().reset_index()
daily_active.columns = ['date', 'daily_active_users']
daily_active['date'] = daily_active['date'].astype(str)

# Plays per session
plays_per_session = listen.groupby('sessionId')['song'].count()
avg_plays_per_session = plays_per_session.mean()

# Age distribution
age_distribution = auth['age'].value_counts().sort_index().reset_index()
age_distribution.columns = ['age', 'user_count']
age_distribution = age_distribution.sort_values(by='age')

# Genre popularity
genre_popularity = listen['genre'].value_counts().reset_index()
genre_popularity.columns = ['genre', 'play_count']

# Top artists (excluding corrupted entries)
top_artists = listen['artist'].value_counts().head(25).reset_index()
top_artists.columns = ['artist', 'play_count']
# Filter out corrupted Björk entries
top_artists = top_artists[~top_artists['artist'].str.contains('BjÃÂ¶rk|Bj.*rk', na=False, regex=True)].head(20)

# Top songs
top_songs = listen.groupby(['artist', 'song']).size().reset_index(name='play_count')
top_songs = top_songs.sort_values('play_count', ascending=False).head(20)

# Top songs by state (top 5 songs per state)
def get_top_songs_by_state(df, top_n=5):
    """Get top N songs for each state"""
    state_songs = df.groupby(['state', 'artist', 'song']).size().reset_index(name='play_count')
    
    # Get top songs per state
    top_songs_by_state = []
    for state in state_songs['state'].unique():
        state_data = state_songs[state_songs['state'] == state].sort_values('play_count', ascending=False).head(top_n)
        state_data['rank'] = range(1, len(state_data) + 1)
        top_songs_by_state.append(state_data)
    
    return pd.concat(top_songs_by_state, ignore_index=True)

top_songs_by_state = get_top_songs_by_state(listen, top_n=5)

# Also create a summary showing #1 song per state
top_song_per_state = top_songs_by_state[top_songs_by_state['rank'] == 1][['state', 'artist', 'song', 'play_count']].reset_index(drop=True)

# Top artists by state (top 5 artists per state)
def get_top_artists_by_state(df, top_n=5):
    """Get top N artists for each state"""
    # Filter out corrupted entries first
    df_clean = df[~df['artist'].str.contains('BjÃÂ¶rk|Bj.*rk', na=False, regex=True)]
    state_artists = df_clean.groupby(['state', 'artist']).size().reset_index(name='play_count')
    
    # Get top artists per state
    top_artists_by_state = []
    for state in state_artists['state'].unique():
        state_data = state_artists[state_artists['state'] == state].sort_values('play_count', ascending=False).head(top_n)
        state_data['rank'] = range(1, len(state_data) + 1)
        top_artists_by_state.append(state_data)
    
    return pd.concat(top_artists_by_state, ignore_index=True)

top_artists_by_state = get_top_artists_by_state(listen, top_n=5)

# Also create a summary showing #1 artist per state
top_artist_per_state = top_artists_by_state[top_artists_by_state['rank'] == 1][['state', 'artist', 'play_count']].reset_index(drop=True)

# User engagement by subscription level
engagement_by_level = listen.groupby('level').agg({
    'userId': 'nunique',
    'song': 'count',
    'duration': 'mean'
}).reset_index()
engagement_by_level.columns = ['subscription_level', 'unique_users', 'total_plays', 'avg_song_duration']

# Geographic analysis
geo_analysis = listen.groupby(['state', 'city']).agg({
    'userId': 'nunique',
    'song': 'count'
}).reset_index()
geo_analysis.columns = ['state', 'city', 'unique_users', 'total_plays']
geo_analysis = geo_analysis.sort_values('total_plays', ascending=False).head(20)

# Hourly listening patterns
hourly_patterns = listen.groupby('hour')['song'].count().reset_index()
hourly_patterns.columns = ['hour', 'total_plays']

# User session analysis
session_analysis = listen.groupby(['userId', 'sessionId']).agg({
    'song': 'count',
    'duration': 'sum'
}).reset_index()
session_analysis.columns = ['userId', 'sessionId', 'songs_played', 'total_duration']

# Combine user data with listening behavior
user_profiles = auth.merge(
    listen.groupby('userId').agg({
        'song': 'count',
        'duration': 'sum',
        'genre': lambda x: x.mode().iloc[0] if not x.empty else 'unknown'
    }).reset_index(),
    on='userId',
    how='left'
)
user_profiles.columns = list(user_profiles.columns[:-3]) + ['total_plays', 'total_listening_time', 'favorite_genre']

print("Compiling final aggregated data structure...")

# Create the final aggregated data structure
aggregated_data = {
    "metadata": {
        "generated_at": datetime.now().isoformat(),
        "total_users": int(len(auth)),
        "total_listen_events": int(len(listen)),
        "total_page_views": int(len(page)),
        "total_status_changes": int(len(status)),
        "date_range": {
            "start": str(min(listen['date'])),
            "end": str(max(listen['date']))
        }
    },
    "user_analytics": {
        "daily_active_users": daily_active.to_dict('records'),
        "age_distribution": age_distribution.to_dict('records'),
        "subscription_levels": {
            "free": int(len(auth[auth['level'] == 'free'])),
            "paid": int(len(auth[auth['level'] == 'paid']))
        }
    },
    "content_analytics": {
        "genre_popularity": genre_popularity.to_dict('records'),
        "top_artists": top_artists.to_dict('records'),
        "top_songs": top_songs.to_dict('records'),
        "top_songs_by_state": top_songs_by_state.to_dict('records'),
        "top_song_per_state": top_song_per_state.to_dict('records'),
        "top_artists_by_state": top_artists_by_state.to_dict('records'),
        "top_artist_per_state": top_artist_per_state.to_dict('records'),
        "average_plays_per_session": float(avg_plays_per_session)
    },
    "engagement_analytics": {
        "by_subscription_level": engagement_by_level.to_dict('records'),
        "hourly_patterns": hourly_patterns.to_dict('records'),
        "geographic_distribution": geo_analysis.to_dict('records')
    },
    "user_profiles": user_profiles.fillna(0).to_dict('records'),
    "top_songs_by_state": top_songs_by_state.fillna(0).to_dict('records'),
    "top_song_per_state": top_song_per_state.fillna(0).to_dict('records')
}

# Save the aggregated data to JSON file
print("Saving aggregated data to JSON file...")
with open('aggregated_music_data.json', 'w') as f:
    json.dump(aggregated_data, f, indent=2, default=str)

# Also save individual CSV files for specific analyses
print("Saving individual analysis files...")
daily_active.to_csv("daily_active_users.csv", index=False)
age_distribution.to_csv("age_distribution.csv", index=False)
genre_popularity.to_csv("genre_popularity.csv", index=False)
top_artists.to_csv("top_artists.csv", index=False)
top_songs.to_csv("top_songs.csv", index=False)
engagement_by_level.to_csv("engagement_by_level.csv", index=False)
geo_analysis.to_csv("geographic_analysis.csv", index=False)
hourly_patterns.to_csv("hourly_patterns.csv", index=False)
top_songs_by_state.to_csv("top_songs_by_state.csv", index=False)
top_song_per_state.to_csv("top_song_per_state.csv", index=False)
top_artists_by_state.to_csv("top_artists_by_state.csv", index=False)
top_artist_per_state.to_csv("top_artist_per_state.csv", index=False)

print("✅ Data aggregation complete!")
print(f"📊 Generated comprehensive JSON file: 'aggregated_music_data.json'")
print(f"📈 Total users analyzed: {len(auth)}")
print(f"🎵 Total listening events: {len(listen)}")
print(f"📱 Total page views: {len(page)}")
print(f"🔄 Total status changes: {len(status)}")
print(f"🎨 Unique genres identified: {len(genre_popularity)}")
print(f"🎤 Unique artists: {len(top_artists)}")
