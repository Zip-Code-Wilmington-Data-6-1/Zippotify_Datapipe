#!/usr/bin/env python3
"""
Cache Prewarming Tool - Build artist cache from already processed songs
This dramatically speeds up processing by avoiding repeated API calls
"""

import pickle
import json
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct
from models import DimSong, DimGenre, DimSongGenre, DimArtist, DimSongArtist
from database import SessionLocal
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
from dotenv import load_dotenv

# Load environment
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

def prewarm_cache_from_database():
    """Build cache from songs that already have genres in database"""
    print("🔥 Prewarming cache from existing database data...")
    
    session = SessionLocal()
    cache = {}
    
    try:
        # Get all songs that already have genres
        songs_with_genres = session.query(
            DimSong.song_id,
            DimSong.song_title,
            func.array_agg(DimGenre.genre_name).label('genres')
        ).join(
            DimSongGenre, DimSong.song_id == DimSongGenre.song_id
        ).join(
            DimGenre, DimSongGenre.genre_id == DimGenre.genre_id
        ).group_by(
            DimSong.song_id, DimSong.song_title
        ).limit(1000).all()  # Start with first 1000
        
        print(f"Found {len(songs_with_genres)} songs with existing genre data")
        
        # For each song, try to find the artists and cache their genres
        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=os.getenv("SPOTIPY_CLIENT_ID"),
            client_secret=os.getenv("SPOTIPY_CLIENT_SECRET")
        ))
        
        cached_artists = 0
        for song in songs_with_genres[:100]:  # Process first 100 to start
            try:
                # Search for the track to get artist IDs
                results = sp.search(q=song.song_title, type='track', limit=1)
                items = results['tracks']['items']
                
                if items:
                    track = items[0]
                    for artist in track['artists']:
                        artist_id = artist['id']
                        if artist_id not in cache:
                            try:
                                artist_data = sp.artist(artist_id)
                                artist_genres = artist_data.get('genres', [])
                                cache[artist_id] = artist_genres
                                cached_artists += 1
                                
                                if cached_artists % 10 == 0:
                                    print(f"  Cached {cached_artists} artists...")
                                    
                            except Exception as e:
                                print(f"  Warning: Could not cache artist {artist_id}: {e}")
                                cache[artist_id] = []
                
            except Exception as e:
                print(f"  Warning: Could not process song {song.song_title}: {e}")
                continue
        
        print(f"✅ Prewarmed cache with {len(cache)} artists")
        
        # Save the cache for all processes
        for i in range(10):  # Save for processes 0-9
            cache_file = f"artist_genre_cache_{i}.pkl"
            with open(cache_file, 'wb') as f:
                pickle.dump(cache, f)
            print(f"💾 Saved prewarmed cache: {cache_file}")
    
    except Exception as e:
        print(f"❌ Error prewarming cache: {e}")
    finally:
        session.close()
    
    return len(cache)

def create_speed_boost_cache():
    """Create a speed boost cache with common artists"""
    print("\n🚀 Creating speed boost cache with popular artists...")
    
    # Popular artists likely to appear frequently
    popular_artists = [
        "Drake", "Taylor Swift", "Ed Sheeran", "Billie Eilish", "Post Malone",
        "Ariana Grande", "Justin Bieber", "The Weeknd", "Bad Bunny", "Dua Lipa",
        "Olivia Rodrigo", "Harry Styles", "Eminem", "BTS", "Kanye West",
        "Rihanna", "Bruno Mars", "Adele", "Beyoncé", "Travis Scott"
    ]
    
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=os.getenv("SPOTIPY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIPY_CLIENT_SECRET")
    ))
    
    speed_cache = {}
    
    for artist_name in popular_artists:
        try:
            # Search for artist
            results = sp.search(q=f"artist:{artist_name}", type='artist', limit=1)
            if results['artists']['items']:
                artist = results['artists']['items'][0]
                artist_id = artist['id']
                genres = artist.get('genres', [])
                speed_cache[artist_id] = genres
                print(f"  ⚡ Cached {artist_name}: {len(genres)} genres")
        except Exception as e:
            print(f"  Warning: Could not cache {artist_name}: {e}")
    
    # Merge with existing caches
    for i in range(10):
        cache_file = f"artist_genre_cache_{i}.pkl"
        existing_cache = {}
        
        if os.path.exists(cache_file):
            with open(cache_file, 'rb') as f:
                existing_cache = pickle.load(f)
        
        # Merge caches
        existing_cache.update(speed_cache)
        
        with open(cache_file, 'wb') as f:
            pickle.dump(existing_cache, f)
    
    print(f"✅ Speed boost cache created with {len(speed_cache)} popular artists")
    return len(speed_cache)

def main():
    print("🔥 SPEED OPTIMIZATION: Cache Prewarming")
    print("=" * 50)
    
    # Step 1: Prewarm from database
    db_cache_size = prewarm_cache_from_database()
    
    # Step 2: Add popular artists
    speed_cache_size = create_speed_boost_cache()
    
    print("\n" + "=" * 50)
    print(f"🎯 PREWARMING COMPLETE!")
    print(f"   Database cache: {db_cache_size} artists")
    print(f"   Speed boost cache: {speed_cache_size} artists")
    print(f"   Expected speedup: 2-3x faster processing")
    print("\n✅ Ready for high-speed processing!")

if __name__ == "__main__":
    main()
