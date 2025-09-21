from dotenv import load_dotenv
import os
import time
import pickle
import random
from datetime import datetime

# Load .env from the current directory or parent if needed
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from sqlalchemy.orm import Session
from models import DimSong, DimGenre, DimSongGenre, FactPlays
from sqlalchemy import func
from database import SessionLocal

CACHE_FILE = "artist_genre_cache.pkl"

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=os.getenv("SPOTIPY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIPY_CLIENT_SECRET")
))

def get_or_create_genre(session, genre_name):
    genre = session.query(DimGenre).filter_by(genre_name=genre_name).first()
    if not genre:
        genre = DimGenre(genre_name=genre_name)
        session.add(genre)
        session.commit()
    return genre

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "rb") as f:
            return pickle.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "wb") as f:
        pickle.dump(cache, f)

def safe_api_call(func, *args, max_retries=3, base_delay=60):
    """Make API calls with exponential backoff and longer delays"""
    for attempt in range(max_retries):
        try:
            result = func(*args)
            return result
        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 429:  # Rate limited
                if 'retry-after' in e.headers:
                    retry_after = int(e.headers['retry-after'])
                else:
                    retry_after = base_delay * (2 ** attempt)  # Exponential backoff
                
                print(f"Rate limited. Attempt {attempt + 1}/{max_retries}. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                
                # Add extra random delay to avoid hitting limits again immediately
                extra_delay = random.randint(30, 60)
                print(f"Adding extra safety delay of {extra_delay} seconds...")
                time.sleep(extra_delay)
            else:
                print(f"Spotify API error: {e}")
                return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None
    
    print(f"Failed after {max_retries} attempts")
    return None

def process_single_song(session, song, genre_cache):
    """Process one song with maximum safety"""
    print(f"Processing song_id={song.song_id}: {song.song_title}")
    
    try:
        # First API call: Search for the track
        print("  Searching for track...")
        results = safe_api_call(sp.search, song.song_title, type='track', limit=1)
        if not results:
            print("  Search failed, skipping")
            return False
            
        items = results['tracks']['items']
        if not items:
            print("  No tracks found, skipping")
            return False
            
        track = items[0]
        artist_ids = [artist['id'] for artist in track['artists']]
        print(f"  Found {len(artist_ids)} artists")
        
        # Process each artist one by one with delays
        song_genres = set()
        for i, artist_id in enumerate(artist_ids):
            if artist_id in genre_cache:
                print(f"  Artist {i+1}/{len(artist_ids)}: Using cached genres")
                song_genres.update(genre_cache[artist_id])
            else:
                print(f"  Artist {i+1}/{len(artist_ids)}: Fetching from API...")
                
                # Second API call: Get artist details
                artist_data = safe_api_call(sp.artist, artist_id)
                if artist_data:
                    genres = artist_data.get('genres', [])
                    genre_cache[artist_id] = genres
                    song_genres.update(genres)
                    print(f"    Found {len(genres)} genres")
                else:
                    print(f"    Failed to fetch artist data")
                
                # Long delay between artist lookups
                if i < len(artist_ids) - 1:  # Don't delay after the last artist
                    delay = random.randint(45, 75)
                    print(f"    Waiting {delay} seconds before next artist...")
                    time.sleep(delay)
        
        # Save genres to database
        if song_genres:
            print(f"  Saving {len(song_genres)} genres to database...")
            for genre_name in song_genres:
                genre = get_or_create_genre(session, genre_name)
                exists = session.query(DimSongGenre).filter_by(song_id=song.song_id, genre_id=genre.genre_id).first()
                if not exists:
                    link = DimSongGenre(song_id=song.song_id, genre_id=genre.genre_id)
                    session.add(link)
            session.commit()
            print(f"  ✅ Successfully processed song with {len(song_genres)} genres")
            return True
        else:
            print("  No genres found")
            return False
            
    except Exception as e:
        print(f"  ❌ Error processing song: {e}")
        session.rollback()
        return False

def main():
    session = SessionLocal()
    genre_cache = load_cache()
    
    # Get total count
    total = session.query(DimSong).count()
    processed_count = 0
    
    print(f"=== ULTRA-SAFE Spotify Genre Processing ===")
    print(f"Target: Process {total:,} songs at maximum safety")
    print(f"Strategy: 1 song at a time, long delays, exponential backoff")
    print(f"Expected time: ~{total * 3 / 60:.0f} hours minimum")
    print()
    
    # Process songs ordered by popularity (most played first)
    songs_query = session.query(DimSong).join(FactPlays, DimSong.song_id == FactPlays.song_id).group_by(DimSong.song_id, DimSong.song_title).order_by(func.count(FactPlays.play_id).desc())
    
    for song in songs_query:
        # Check if already processed
        existing = session.query(DimSongGenre).filter_by(song_id=song.song_id).first()
        if existing:
            print(f"Song {song.song_id} already has genres, skipping")
            continue
            
        start_time = time.time()
        success = process_single_song(session, song, genre_cache)
        
        if success:
            processed_count += 1
            
        # Save cache after each song
        save_cache(genre_cache)
        
        elapsed = time.time() - start_time
        print(f"Song completed in {elapsed:.1f} seconds")
        
        # Ultra-long delay between songs
        delay = random.randint(120, 180)  # 2-3 minutes between songs
        print(f"Waiting {delay} seconds before next song... (Processed: {processed_count})")
        print(f"Time: {datetime.now().strftime('%H:%M:%S')} | Progress: {processed_count}/{total}")
        print("-" * 60)
        time.sleep(delay)
    
    print(f"Completed! Processed {processed_count} songs.")
    session.close()

if __name__ == "__main__":
    main()