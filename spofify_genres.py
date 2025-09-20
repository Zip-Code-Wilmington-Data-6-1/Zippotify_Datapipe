from dotenv import load_dotenv
import os

# Load .env from the current directory or parent if needed
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import spotipy
import time
import pickle
from spotipy.oauth2 import SpotifyClientCredentials
from sqlalchemy.orm import Session
from models import DimSong, DimGenre, DimSongGenre, FactPlays
from sqlalchemy import func
from database import SessionLocal
from concurrent.futures import ThreadPoolExecutor, as_completed

CACHE_FILE = "artist_genre_cache.pkl"
MAX_WORKERS = 1  # Reduced to avoid rate limiting

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

def fetch_artist_genres(artist_id, genre_cache):
    if artist_id in genre_cache:
        return artist_id, genre_cache[artist_id]
    while True:
        try:
            artist = sp.artist(artist_id)
            genres = artist.get('genres', [])
            genre_cache[artist_id] = genres
            return artist_id, genres
        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", 5))
                print(f"Rate limited. Sleeping for {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                print(f"Spotify error: {e}")
                return artist_id, []
        except Exception as e:
            print(f"Error fetching artist {artist_id}: {e}")
            return artist_id, []

def process_batch(session, songs, genre_cache):
    for song in songs:
        print(f"Processing song_id={song.song_id}: {song.song_title}")
        try:
            results = sp.search(q=song.song_title, type='track', limit=1)
            items = results['tracks']['items']
            if not items:
                continue
            track = items[0]
            artist_ids = [artist['id'] for artist in track['artists']]

            # Fetch genres for all artists in parallel
            song_genres = set()
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(fetch_artist_genres, artist_id, genre_cache) for artist_id in artist_ids]
                for future in as_completed(futures):
                    _, genres = future.result()
                    song_genres.update(genres)

            for genre_name in song_genres:
                genre = get_or_create_genre(session, genre_name)
                exists = session.query(DimSongGenre).filter_by(song_id=song.song_id, genre_id=genre.genre_id).first()
                if not exists:
                    link = DimSongGenre(song_id=song.song_id, genre_id=genre.genre_id)
                    session.add(link)
            session.commit()
            time.sleep(3.0)  # Much longer delay to avoid rate limiting
        except Exception as e:
            print(f"Error processing song_id={song.song_id}: {e}")
            session.rollback()

def main():
    session = SessionLocal()
    batch_size = 10  # Much smaller batches to avoid rate limiting
    offset = 0
    total = session.query(DimSong).count()
    genre_cache = load_cache()

    print(f"\nProcessing ALL {total} songs in the database (very slowly to avoid rate limits)")
    
    while offset < total:
        print(f"Processing batch {offset} to {min(offset+batch_size, total)} of {total}")
        # Get songs ordered by how often they're played (most popular first)
        songs = session.query(DimSong).join(FactPlays, DimSong.song_id == FactPlays.song_id).group_by(DimSong.song_id, DimSong.song_title).order_by(func.count(FactPlays.play_id).desc()).offset(offset).limit(batch_size).all()
        
        if not songs:
            break
            
        process_batch(session, songs, genre_cache)
        save_cache(genre_cache)  # Save cache after each batch
        
        offset += batch_size
        
        # Much longer delay between batches to avoid rate limiting
        print("Waiting 60 seconds between batches to be API-friendly...")
        time.sleep(60)

    print(f"Completed processing all {total} songs!")
    session.close()

if __name__ == "__main__":
    main()