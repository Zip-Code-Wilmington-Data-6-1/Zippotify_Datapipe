from dotenv import load_dotenv
import os
import time
import pickle
import json
import random
from pathlib import Path
import argparse
from datetime import datetime
import logging

# Load .env from the current directory or parent if needed
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from models import DimSong, DimGenre, DimSongGenre
from database import SessionLocal, engine
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'spotify_genres_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SpotifyGenreProcessor:
    def __init__(self, process_id=0, max_workers=3):
        self.process_id = process_id
        self.max_workers = max_workers
        self.cache_file = f"artist_genre_cache_{process_id}.pkl"
        self.progress_file = f"progress_{process_id}.json"
        self.lock = threading.Lock()
        
        # Initialize Spotify client with exponential backoff
        self.sp = self._init_spotify_client()
        self.genre_cache = self.load_cache()
        self.request_count = 0
        self.start_time = time.time()
        
    def _init_spotify_client(self):
        """Initialize Spotify client with retry logic"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
                    client_id=os.getenv("SPOTIPY_CLIENT_ID"),
                    client_secret=os.getenv("SPOTIPY_CLIENT_SECRET")
                ))
                # Test the connection
                sp.search(q="test", type='track', limit=1)
                logger.info(f"Spotify client initialized successfully (attempt {attempt + 1})")
                return sp
            except Exception as e:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"Failed to initialize Spotify client (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                else:
                    raise
    
    def load_cache(self):
        """Load cache from file"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "rb") as f:
                    cache = pickle.load(f)
                logger.info(f"Loaded cache with {len(cache)} entries")
                return cache
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}")
        return {}
    
    def save_cache(self):
        """Save cache to file with atomic write"""
        temp_file = f"{self.cache_file}.tmp"
        try:
            with open(temp_file, "wb") as f:
                pickle.dump(self.genre_cache, f)
            os.rename(temp_file, self.cache_file)
            logger.info(f"Cache saved with {len(self.genre_cache)} entries")
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
    
    def save_progress(self, current_id, total_processed):
        """Save progress to file"""
        progress = {
            "current_id": current_id,
            "total_processed": total_processed,
            "timestamp": datetime.now().isoformat(),
            "cache_size": len(self.genre_cache)
        }
        try:
            with open(self.progress_file, "w") as f:
                json.dump(progress, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save progress: {e}")
    
    def load_progress(self):
        """Load progress from file"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load progress: {e}")
        return None
    
    def rate_limit_check(self):
        """Implement intelligent rate limiting"""
        self.request_count += 1
        
        # Calculate requests per minute
        elapsed = time.time() - self.start_time
        rpm = (self.request_count / elapsed) * 60 if elapsed > 0 else 0
        
        # Spotify allows ~100 requests per minute, we'll be conservative
        if rpm > 80:
            sleep_time = 60 / 80  # Throttle to 80 requests per minute
            time.sleep(sleep_time)
            
        # Reset counters every hour
        if elapsed > 3600:
            self.request_count = 0
            self.start_time = time.time()
    
    def fetch_artist_genres_with_retry(self, artist_id, max_retries=5):
        """Fetch artist genres with exponential backoff"""
        if artist_id in self.genre_cache:
            return self.genre_cache[artist_id]
        
        for attempt in range(max_retries):
            try:
                self.rate_limit_check()
                artist = self.sp.artist(artist_id)
                genres = artist.get('genres', [])
                
                with self.lock:
                    self.genre_cache[artist_id] = genres
                
                return genres
                
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:
                    # Rate limited
                    retry_after = int(e.headers.get("Retry-After", 60))
                    jitter = random.uniform(0, 10)
                    sleep_time = retry_after + jitter
                    logger.warning(f"Rate limited. Sleeping for {sleep_time:.1f} seconds...")
                    time.sleep(sleep_time)
                    continue
                elif e.http_status == 404:
                    # Artist not found
                    logger.warning(f"Artist {artist_id} not found")
                    with self.lock:
                        self.genre_cache[artist_id] = []
                    return []
                else:
                    wait_time = (2 ** attempt) + random.uniform(0, 5)
                    logger.warning(f"Spotify error (attempt {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                    continue
            except Exception as e:
                wait_time = (2 ** attempt) + random.uniform(0, 5)
                logger.error(f"Unexpected error fetching artist {artist_id} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                continue
        
        # If all retries failed
        logger.error(f"Failed to fetch artist {artist_id} after {max_retries} attempts")
        with self.lock:
            self.genre_cache[artist_id] = []
        return []
    
    def search_track_with_retry(self, song_title, max_retries=3):
        """Search for track with retry logic"""
        for attempt in range(max_retries):
            try:
                self.rate_limit_check()
                results = self.sp.search(q=song_title, type='track', limit=1)
                return results['tracks']['items']
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:
                    retry_after = int(e.headers.get("Retry-After", 60))
                    jitter = random.uniform(0, 10)
                    sleep_time = retry_after + jitter
                    logger.warning(f"Rate limited on search. Sleeping for {sleep_time:.1f} seconds...")
                    time.sleep(sleep_time)
                    continue
                else:
                    wait_time = (2 ** attempt) + random.uniform(0, 3)
                    logger.warning(f"Search error (attempt {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                    continue
            except Exception as e:
                wait_time = (2 ** attempt) + random.uniform(0, 3)
                logger.error(f"Unexpected search error (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                continue
        
        return []
    
    def get_or_create_genre(self, session, genre_name):
        """Get or create genre with proper error handling"""
        try:
            # Try to get existing genre first
            genre = session.query(DimGenre).filter_by(genre_name=genre_name).first()
            if genre:
                return genre
            
            # Create new genre
            genre = DimGenre(genre_name=genre_name)
            session.add(genre)
            session.flush()  # Get the ID without committing
            return genre
            
        except Exception as e:
            session.rollback()
            # Try again in case of race condition
            genre = session.query(DimGenre).filter_by(genre_name=genre_name).first()
            if genre:
                return genre
            raise e
    
    def get_processed_song_ids(self, session, song_ids):
        """Get set of song IDs that have already been processed"""
        processed_ids = set()
        if song_ids:
            # Query for songs that already have genre entries
            result = session.query(DimSongGenre.song_id).filter(
                DimSongGenre.song_id.in_(song_ids)
            ).distinct().all()
            processed_ids = {row[0] for row in result}
        return processed_ids

    def process_song(self, song, already_processed=False):
        """Process a single song"""
        try:
            # Skip if we already know it's processed
            if already_processed:
                return f"Skipped song_id={song.song_id} (already processed)"
            
            # Search for track
            items = self.search_track_with_retry(song.song_title)
            if not items:
                return f"No track found for song_id={song.song_id}: {song.song_title}"
            
            track = items[0]
            artist_ids = [artist['id'] for artist in track['artists']]
            
            # Collect all genres from all artists
            all_genres = set()
            for artist_id in artist_ids:
                genres = self.fetch_artist_genres_with_retry(artist_id)
                all_genres.update(genres)
            
            if not all_genres:
                return f"No genres found for song_id={song.song_id}"
            
            # Create genre entries and links
            session = SessionLocal()
            try:
                # Double-check if processed while we were making API calls
                existing = session.query(DimSongGenre).filter_by(song_id=song.song_id).first()
                if existing:
                    session.close()
                    return f"Skipped song_id={song.song_id} (processed by another process)"
                
                for genre_name in all_genres:
                    genre = self.get_or_create_genre(session, genre_name)
                    
                    # Check if link already exists (race condition safety)
                    existing_link = session.query(DimSongGenre).filter_by(
                        song_id=song.song_id, 
                        genre_id=genre.genre_id
                    ).first()
                    
                    if not existing_link:
                        link = DimSongGenre(song_id=song.song_id, genre_id=genre.genre_id)
                        session.add(link)
                
                session.commit()
                session.close()
                
                return f"Processed song_id={song.song_id}: {len(all_genres)} genres"
                
            except Exception as e:
                session.rollback()
                session.close()
                return f"Database error for song_id={song.song_id}: {e}"
                
        except Exception as e:
            return f"Error processing song_id={song.song_id}: {e}"
    
    def get_processing_stats(self, session, min_id, max_id):
        """Get statistics about what's already processed in the range"""
        # Count total songs in range
        total_songs = session.query(DimSong).filter(
            and_(DimSong.song_id >= min_id, DimSong.song_id <= max_id)
        ).count()
        
        # Count already processed songs in range
        processed_count = session.query(DimSong.song_id).join(
            DimSongGenre, DimSong.song_id == DimSongGenre.song_id
        ).filter(
            and_(DimSong.song_id >= min_id, DimSong.song_id <= max_id)
        ).distinct().count()
        
        return total_songs, processed_count

    def process_batch(self, min_id, max_id, batch_size=50):
        """Process songs in batches"""
        session = SessionLocal()
        
        try:
            # Get processing statistics
            total_songs, already_processed = self.get_processing_stats(session, min_id, max_id)
            remaining_songs = total_songs - already_processed
            
            logger.info(f"Processing range {min_id} to {max_id}:")
            logger.info(f"  Total songs in range: {total_songs:,}")
            percentage = (already_processed/total_songs*100) if total_songs > 0 else 0
            logger.info(f"  Already processed: {already_processed:,} ({percentage:.1f}%)")
            logger.info(f"  Remaining to process: {remaining_songs:,}")
            
            if remaining_songs == 0:
                logger.info("All songs in this range are already processed!")
                return
            
            # Load progress if available
            progress = self.load_progress()
            start_id = progress.get("current_id", min_id) if progress else min_id
            total_processed = progress.get("total_processed", already_processed) if progress else already_processed
            
            logger.info(f"Starting batch processing from ID {start_id}")
            
            current_id = start_id
            save_interval = 100  # Save progress every 100 songs
            
            while current_id <= max_id:
                # Get batch of songs
                songs = session.query(DimSong).filter(
                    and_(
                        DimSong.song_id >= current_id,
                        DimSong.song_id < current_id + batch_size,
                        DimSong.song_id <= max_id
                    )
                ).all()
                
                if not songs:
                    current_id += batch_size
                    continue
                
                # Check which songs are already processed (batch operation)
                song_ids = [song.song_id for song in songs]
                processed_song_ids = self.get_processed_song_ids(session, song_ids)
                
                # Filter out already processed songs
                songs_to_process = []
                skipped_count = 0
                
                for song in songs:
                    if song.song_id in processed_song_ids:
                        skipped_count += 1
                        total_processed += 1  # Count as processed for progress tracking
                    else:
                        songs_to_process.append(song)
                
                if skipped_count > 0:
                    logger.info(f"Batch {current_id}: Skipped {skipped_count} already processed songs")
                
                if not songs_to_process:
                    logger.info(f"Batch {current_id}: All {len(songs)} songs already processed")
                    current_id += batch_size
                    continue
                
                logger.info(f"Processing batch: {len(songs_to_process)} new songs (of {len(songs)} total) starting from ID {current_id}")
                
                # Process only unprocessed songs with limited concurrency
                results = []
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_song = {
                        executor.submit(self.process_song, song, False): song 
                        for song in songs_to_process
                    }
                    
                    for future in as_completed(future_to_song):
                        result = future.result()
                        results.append(result)
                        total_processed += 1
                        
                        if total_processed % 10 == 0:
                            logger.info(f"Progress: {total_processed} songs processed")
                
                # Save progress and cache periodically
                if total_processed % save_interval == 0:
                    self.save_progress(current_id + batch_size, total_processed)
                    self.save_cache()
                
                current_id += batch_size
                
                # Small delay between batches to be respectful
                time.sleep(1)
            
            # Final save
            self.save_progress(max_id, total_processed)
            self.save_cache()
            
            logger.info(f"Batch processing completed. Total processed: {total_processed}")
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            raise
        finally:
            session.close()

def main():
    parser = argparse.ArgumentParser(description="Process Spotify genre data")
    parser.add_argument("--min_id", type=int, required=True, help="Minimum song ID to process")
    parser.add_argument("--max_id", type=int, required=True, help="Maximum song ID to process")
    parser.add_argument("--process_id", type=int, default=0, help="Process ID for cache file naming")
    parser.add_argument("--max_workers", type=int, default=3, help="Maximum concurrent workers")
    parser.add_argument("--batch_size", type=int, default=50, help="Batch size for processing")
    
    args = parser.parse_args()
    
    logger.info(f"Starting Spotify genre processing:")
    logger.info(f"  ID range: {args.min_id} - {args.max_id}")
    logger.info(f"  Process ID: {args.process_id}")
    logger.info(f"  Max workers: {args.max_workers}")
    logger.info(f"  Batch size: {args.batch_size}")
    
    processor = SpotifyGenreProcessor(
        process_id=args.process_id,
        max_workers=args.max_workers
    )
    
    try:
        processor.process_batch(args.min_id, args.max_id, args.batch_size)
        logger.info("Processing completed successfully!")
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        processor.save_cache()
        processor.save_progress(args.min_id, 0)
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        processor.save_cache()
        raise

if __name__ == "__main__":
    main()