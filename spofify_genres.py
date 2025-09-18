from dotenv import load_dotenv
import os

# Load .env from the current directory or parent if needed
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import spotipy
import time
import pickle
from spotipy.oauth2 import SpotifyClientCredentials
from sqlalchemy.orm import Session
from models import DimSong, DimGenre, DimSongGenre
from database import SessionLocal
from concurrent.futures import ThreadPoolExecutor, as_completed

CACHE_FILE = "artist_genre_cache.pkl"
MAX_WORKERS = 10  # Tune this for your rate limit comfort

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
            time.sleep(0.1)  # Be nice to the API
        except Exception as e:
            print(f"Error processing song_id={song.song_id}: {e}")
            session.rollback()

def main():
    session = SessionLocal()
    batch_size = 1000  # Only process 1000 songs for testing
    offset = 0
    total = session.query(DimSong).count()
    genre_cache = load_cache()

    print(f"\nProcessing batch {offset} to {offset+batch_size} of {total} (LIMITED TO 1000 SONGS FOR TEST)")
    songs = session.query(DimSong).order_by(DimSong.song_id).offset(offset).limit(batch_size).all()
    if songs:
        process_batch(session, songs, genre_cache)
        save_cache(genre_cache)

    session.close()

if __name__ == "__main__":
    main()