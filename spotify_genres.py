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
from multiprocessing import Process
import argparse

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
    # Preload all genres into a dict
    all_genres = {g.genre_name: g for g in session.query(DimGenre).all()}
    new_genres = []
    new_links = []

    # Preload all existing song-genre links for this batch
    song_ids = [song.song_id for song in songs]
    existing_links = set(
        (sg.song_id, sg.genre_id)
        for sg in session.query(DimSongGenre).filter(DimSongGenre.song_id.in_(song_ids)).all()
    )

    processed_song_ids = {sg[0] for sg in existing_links}
    for song in songs:
        print(f"Processing song_id={song.song_id}: {song.song_title}")
        try:
            if song.song_id in processed_song_ids:
                print(f"Skipping song_id={song.song_id} (already processed)")
                continue

            results = sp.search(q=song.song_title, type='track', limit=1)
            items = results['tracks']['items']
            if not items:
                continue
            track = items[0]
            artist_ids = [artist['id'] for artist in track['artists']]

            song_genres = set()
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(fetch_artist_genres, artist_id, genre_cache) for artist_id in artist_ids]
                for future in as_completed(futures):
                    _, genres = future.result()
                    song_genres.update(genres)

            for genre_name in song_genres:
                genre = all_genres.get(genre_name)
                if not genre:
                    genre = DimGenre(genre_name=genre_name)
                    new_genres.append(genre)
                    all_genres[genre_name] = genre
                if (song.song_id, genre.genre_id) not in existing_links:
                    link = DimSongGenre(song_id=song.song_id, genre=genre)
                    new_links.append(link)
        except Exception as e:
            print(f"Error processing song_id={song.song_id}: {e}")
            session.rollback()

    if new_genres:
        session.add_all(new_genres)
        session.commit()
        # Update all_genres with DB-generated IDs
        for genre in new_genres:
            all_genres[genre.genre_name] = genre

    if new_links:
        session.add_all(new_links)
        session.commit()

def process_song_chunk(offset, batch_size, total):
    session = SessionLocal()
    genre_cache = load_cache()
    print(f"\n[Process {offset//batch_size}] Processing batch {offset} to {min(offset+batch_size, total)} of {total}")
    songs = session.query(DimSong).order_by(DimSong.song_id).offset(offset).limit(batch_size).all()
    if songs:
        process_batch(session, songs, genre_cache)
        save_cache(genre_cache)
    session.close()

def main(min_id=None, max_id=None):
    session = SessionLocal()
    batch_size = 2000
    query = session.query(DimSong)
    if min_id is not None:
        query = query.filter(DimSong.song_id >= min_id)
    if max_id is not None:
        query = query.filter(DimSong.song_id <= max_id)
    total = query.count()
    session.close()

    num_workers = 8  # Use all CPU cores

    processes = []
    for offset in range(0, total, batch_size):
        p = Process(target=process_song_chunk, args=(offset, batch_size, total))
        p.start()
        processes.append(p)
        if len(processes) >= num_workers:
            for proc in processes:
                proc.join()
            processes = []
    for proc in processes:
        proc.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--min_id", type=int, default=None)
    parser.add_argument("--max_id", type=int, default=None)
    args = parser.parse_args()
    main(min_id=args.min_id, max_id=args.max_id)