from dotenv import load_dotenv
import os
import time
import pickle
import json
import random
from pathlib import Path
import argparse
from datetime import datetime, timedelta
import logging
from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
import threading
from collections import defaultdict, deque
import hashlib

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

class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"         # Failing, reject requests
    HALF_OPEN = "half_open" # Testing if service recovered

@dataclass
class RateLimitInfo:
    remaining: int = 0
    reset_time: Optional[float] = None
    limit: int = 100
    window_seconds: int = 60

@dataclass
class ProcessingCheckpoint:
    song_id: int
    batch_start: int
    timestamp: str
    completed_songs: List[int]
    failed_songs: List[int]
    cache_size: int

class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout_seconds=300, success_threshold=3):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.success_threshold = success_threshold
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
        self.lock = threading.Lock()
    
    def can_execute(self) -> bool:
        with self.lock:
            if self.state == CircuitState.CLOSED:
                return True
            elif self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time >= self.timeout_seconds:
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    return True
                return False
            else:  # HALF_OPEN
                return True
    
    def record_success(self):
        with self.lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
            elif self.state == CircuitState.CLOSED:
                self.failure_count = max(0, self.failure_count - 1)  # Slowly recover
    
    def record_failure(self):
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN
            elif self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN

class AdaptiveRateLimiter:
    def __init__(self, process_id: int, initial_rate=80):
        self.process_id = process_id
        self.target_rpm = initial_rate  # Start conservative
        self.current_rate = 0
        self.request_times = deque()
        self.rate_limit_info = RateLimitInfo()
        self.lock = threading.Lock()
        self.adaptive_factor = 0.8  # Be more conservative when we detect limits
        
    def update_from_headers(self, headers: Dict[str, str]):
        """Update rate limit info from Spotify response headers"""
        with self.lock:
            if 'X-RateLimit-Remaining' in headers:
                self.rate_limit_info.remaining = int(headers['X-RateLimit-Remaining'])
            if 'X-RateLimit-Limit' in headers:
                self.rate_limit_info.limit = int(headers['X-RateLimit-Limit'])
            if 'X-RateLimit-Reset' in headers:
                self.rate_limit_info.reset_time = float(headers['X-RateLimit-Reset'])
            
            # Adjust target rate based on remaining quota
            if self.rate_limit_info.remaining < 10:
                self.target_rpm = max(20, self.target_rpm * 0.5)  # Slow down significantly
            elif self.rate_limit_info.remaining > 50:
                self.target_rpm = min(90, self.target_rpm * 1.1)  # Speed up gradually
    
    def should_wait(self) -> float:
        """Calculate how long to wait before next request"""
        with self.lock:
            now = time.time()
            
            # Clean old request times (older than 60 seconds)
            while self.request_times and now - self.request_times[0] > 60:
                self.request_times.popleft()
            
            self.request_times.append(now)
            current_rpm = len(self.request_times)
            
            if current_rpm >= self.target_rpm:
                # Calculate wait time to stay under limit
                wait_time = 60.0 / self.target_rpm
                return wait_time + random.uniform(0, 0.5)  # Add jitter
            
            return 0

class SmartSpotifyClient:
    def __init__(self, process_id: int):
        self.process_id = process_id
        self.circuit_breaker = CircuitBreaker()
        self.rate_limiter = AdaptiveRateLimiter(process_id)
        self.sp = None
        self.request_cache = {}  # In-memory request cache
        self.failed_requests = set()  # Track failed requests to avoid retrying immediately
        self.lock = threading.Lock()
        self.logger = logging.getLogger(f"spotify_client_{process_id}")
        
        self._init_client()
    
    def _init_client(self):
        """Initialize Spotify client with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
                    client_id=os.getenv("SPOTIPY_CLIENT_ID"),
                    client_secret=os.getenv("SPOTIPY_CLIENT_SECRET")
                ))
                # Test the connection
                self.sp.search(q="test", type='track', limit=1)
                self.logger.info(f"Spotify client initialized successfully")
                return
            except Exception as e:
                wait_time = (2 ** attempt) * (1 + random.random())
                self.logger.warning(f"Failed to initialize Spotify client (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                else:
                    raise
    
    def _get_cache_key(self, method: str, **kwargs) -> str:
        """Generate cache key for request"""
        key_data = f"{method}:{json.dumps(kwargs, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _make_request_with_retry(self, method_name: str, max_retries: int = 5, **kwargs):
        """Make Spotify API request with intelligent retry logic"""
        cache_key = self._get_cache_key(method_name, **kwargs)
        
        # Check cache first
        if cache_key in self.request_cache:
            return self.request_cache[cache_key]
        
        # Check if this request recently failed
        if cache_key in self.failed_requests:
            raise Exception(f"Request recently failed, skipping: {method_name}")
        
        if not self.circuit_breaker.can_execute():
            raise Exception("Circuit breaker is open, service appears to be down")
        
        for attempt in range(max_retries):
            try:
                # Adaptive rate limiting
                wait_time = self.rate_limiter.should_wait()
                if wait_time > 0:
                    time.sleep(wait_time)
                
                # Make the actual request
                method = getattr(self.sp, method_name)
                response = method(**kwargs)
                
                # Cache successful response
                with self.lock:
                    self.request_cache[cache_key] = response
                    self.failed_requests.discard(cache_key)
                
                self.circuit_breaker.record_success()
                return response
                
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:
                    # Rate limited - intelligent backoff
                    retry_after = int(e.headers.get("Retry-After", 60))
                    jitter = random.uniform(0.8, 1.2)
                    backoff_time = min(retry_after * jitter, 300)  # Cap at 5 minutes
                    
                    self.logger.warning(f"Rate limited. Backing off for {backoff_time:.1f}s (attempt {attempt + 1})")
                    
                    # Update rate limiter
                    self.rate_limiter.update_from_headers(e.headers)
                    
                    if backoff_time > 60:  # If long wait, mark circuit breaker
                        self.circuit_breaker.record_failure()
                    
                    time.sleep(backoff_time)
                    continue
                    
                elif e.http_status == 404:
                    # Not found - cache the failure and don't retry
                    with self.lock:
                        self.failed_requests.add(cache_key)
                    raise
                    
                elif e.http_status >= 500:
                    # Server error - exponential backoff
                    wait_time = min((2 ** attempt) * (1 + random.random()), 60)
                    self.logger.warning(f"Server error {e.http_status} (attempt {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                        continue
                    else:
                        self.circuit_breaker.record_failure()
                        raise
                else:
                    # Other client errors
                    self.logger.error(f"Client error {e.http_status}: {e}")
                    with self.lock:
                        self.failed_requests.add(cache_key)
                    raise
                    
            except Exception as e:
                wait_time = min((1.5 ** attempt) * (1 + random.random()), 30)
                self.logger.error(f"Unexpected error (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                    continue
                else:
                    self.circuit_breaker.record_failure()
                    raise
        
        # All retries exhausted
        with self.lock:
            self.failed_requests.add(cache_key)
        raise Exception(f"All retries exhausted for {method_name}")
    
    def search(self, **kwargs):
        return self._make_request_with_retry("search", **kwargs)
    
    def artist(self, artist_id):
        return self._make_request_with_retry("artist", artist_id=artist_id)

class OptimizedSpotifyProcessor:
    def __init__(self, process_id=0, max_workers=2):  # Reduced workers for better rate limiting
        self.process_id = process_id
        self.max_workers = max_workers
        self.cache_file = f"artist_genre_cache_{process_id}.pkl"
        self.progress_file = f"progress_{process_id}.json"
        self.checkpoint_file = f"checkpoint_{process_id}.json"
        self.failed_songs_file = f"failed_songs_{process_id}.json"
        
        # Advanced caching
        self.genre_cache = self.load_cache()
        self.failed_songs = self.load_failed_songs()
        self.completed_songs = set()
        
        # Smart client
        self.spotify_client = SmartSpotifyClient(process_id)
        
        # Logging
        self.logger = logging.getLogger(f"processor_{process_id}")
        
        # Performance tracking
        self.stats = {
            'api_calls': 0,
            'cache_hits': 0,
            'failures': 0,
            'skipped': 0,
            'processed': 0
        }
    
    def load_cache(self) -> Dict:
        """Load cache from file"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "rb") as f:
                    cache = pickle.load(f)
                self.logger.info(f"Loaded cache with {len(cache)} entries")
                return cache
            except Exception as e:
                self.logger.warning(f"Failed to load cache: {e}")
        return {}
    
    def load_failed_songs(self) -> Set[int]:
        """Load list of songs that previously failed"""
        if os.path.exists(self.failed_songs_file):
            try:
                with open(self.failed_songs_file, "r") as f:
                    failed = set(json.load(f))
                self.logger.info(f"Loaded {len(failed)} failed songs to skip")
                return failed
            except Exception as e:
                self.logger.warning(f"Failed to load failed songs: {e}")
        return set()
    
    def save_cache(self):
        """Save cache with atomic write"""
        temp_file = f"{self.cache_file}.tmp"
        try:
            with open(temp_file, "wb") as f:
                pickle.dump(self.genre_cache, f)
            os.rename(temp_file, self.cache_file)
            self.logger.info(f"Cache saved with {len(self.genre_cache)} entries")
        except Exception as e:
            self.logger.error(f"Failed to save cache: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
    
    def save_failed_songs(self):
        """Save failed songs list"""
        try:
            with open(self.failed_songs_file, "w") as f:
                json.dump(list(self.failed_songs), f)
        except Exception as e:
            self.logger.error(f"Failed to save failed songs: {e}")
    
    def save_checkpoint(self, current_song_id: int, batch_start: int, completed: List[int], failed: List[int]):
        """Save detailed checkpoint"""
        checkpoint = ProcessingCheckpoint(
            song_id=current_song_id,
            batch_start=batch_start,
            timestamp=datetime.now().isoformat(),
            completed_songs=completed,
            failed_songs=failed,
            cache_size=len(self.genre_cache)
        )
        
        try:
            with open(self.checkpoint_file, "w") as f:
                json.dump(checkpoint.__dict__, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")
    
    def load_checkpoint(self) -> Optional[ProcessingCheckpoint]:
        """Load checkpoint if exists"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, "r") as f:
                    data = json.load(f)
                    return ProcessingCheckpoint(**data)
            except Exception as e:
                self.logger.warning(f"Failed to load checkpoint: {e}")
        return None
    
    def get_processed_song_ids(self, session, song_ids):
        """Get set of song IDs that have already been processed"""
        if not song_ids:
            return set()
        
        result = session.query(DimSongGenre.song_id).filter(
            DimSongGenre.song_id.in_(song_ids)
        ).distinct().all()
        return {row[0] for row in result}
    
    def fetch_artist_genres(self, artist_id: str) -> List[str]:
        """Fetch artist genres with caching and error handling"""
        if artist_id in self.genre_cache:
            self.stats['cache_hits'] += 1
            return self.genre_cache[artist_id]
        
        try:
            self.stats['api_calls'] += 1
            artist_data = self.spotify_client.artist(artist_id)
            genres = artist_data.get('genres', [])
            
            # Cache the result
            self.genre_cache[artist_id] = genres
            return genres
            
        except Exception as e:
            self.logger.warning(f"Failed to fetch genres for artist {artist_id}: {e}")
            # Cache empty result to avoid retrying
            self.genre_cache[artist_id] = []
            return []
    
    def search_track(self, song_title: str) -> List:
        """Search for track with caching"""
        try:
            self.stats['api_calls'] += 1
            results = self.spotify_client.search(q=song_title, type='track', limit=1)
            return results['tracks']['items']
        except Exception as e:
            self.logger.warning(f"Failed to search for track '{song_title}': {e}")
            return []
    
    def process_song_safe(self, song) -> Tuple[bool, str]:
        """Process a single song with comprehensive error handling"""
        song_id = song.song_id
        
        # Skip if previously failed and not enough time has passed
        if song_id in self.failed_songs:
            self.stats['skipped'] += 1
            return True, f"Skipped song_id={song_id} (previously failed)"
        
        try:
            # Search for track
            items = self.search_track(song.song_title)
            if not items:
                self.failed_songs.add(song_id)
                return False, f"No track found for song_id={song_id}"
            
            track = items[0]
            artist_ids = [artist['id'] for artist in track['artists']]
            
            # Collect genres from all artists
            all_genres = set()
            for artist_id in artist_ids:
                genres = self.fetch_artist_genres(artist_id)
                all_genres.update(genres)
            
            if not all_genres:
                self.failed_songs.add(song_id)
                return False, f"No genres found for song_id={song_id}"
            
            # Save to database
            session = SessionLocal()
            try:
                # Double-check if already processed
                existing = session.query(DimSongGenre).filter_by(song_id=song_id).first()
                if existing:
                    session.close()
                    return True, f"Skipped song_id={song_id} (processed by another process)"
                
                # Create genre entries and links
                for genre_name in all_genres:
                    # Get or create genre
                    genre = session.query(DimGenre).filter_by(genre_name=genre_name).first()
                    if not genre:
                        genre = DimGenre(genre_name=genre_name)
                        session.add(genre)
                        session.flush()
                    
                    # Create link if not exists
                    existing_link = session.query(DimSongGenre).filter_by(
                        song_id=song_id, genre_id=genre.genre_id
                    ).first()
                    
                    if not existing_link:
                        link = DimSongGenre(song_id=song_id, genre_id=genre.genre_id)
                        session.add(link)
                
                session.commit()
                session.close()
                
                self.completed_songs.add(song_id)
                self.stats['processed'] += 1
                return True, f"Processed song_id={song_id}: {len(all_genres)} genres"
                
            except Exception as e:
                session.rollback()
                session.close()
                self.failed_songs.add(song_id)
                return False, f"Database error for song_id={song_id}: {e}"
                
        except Exception as e:
            self.failed_songs.add(song_id)
            self.stats['failures'] += 1
            return False, f"Error processing song_id={song_id}: {e}"
    
    def process_batch_optimized(self, min_id: int, max_id: int, batch_size: int = 25):
        """Process songs with advanced optimization"""
        session = SessionLocal()
        
        try:
            # Load checkpoint if exists
            checkpoint = self.load_checkpoint()
            if checkpoint:
                self.logger.info(f"Resuming from checkpoint: song_id={checkpoint.song_id}")
                self.completed_songs = set(checkpoint.completed_songs)
                self.failed_songs.update(checkpoint.failed_songs)
            
            current_id = checkpoint.song_id if checkpoint else min_id
            total_processed = len(self.completed_songs)
            
            self.logger.info(f"Starting optimized processing from ID {current_id} to {max_id}")
            self.logger.info(f"Already completed: {len(self.completed_songs)}, Failed: {len(self.failed_songs)}")
            
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
                
                # Filter out already processed and failed songs
                song_ids = [song.song_id for song in songs]
                db_processed = self.get_processed_song_ids(session, song_ids)
                
                songs_to_process = []
                for song in songs:
                    if (song.song_id not in db_processed and 
                        song.song_id not in self.completed_songs and
                        song.song_id not in self.failed_songs):
                        songs_to_process.append(song)
                
                skipped = len(songs) - len(songs_to_process)
                if skipped > 0:
                    self.logger.info(f"Batch {current_id}: Skipped {skipped} already processed/failed songs")
                
                if songs_to_process:
                    self.logger.info(f"Processing {len(songs_to_process)} new songs from batch {current_id}")
                    
                    # Process with limited concurrency
                    batch_completed = []
                    batch_failed = []
                    
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        future_to_song = {
                            executor.submit(self.process_song_safe, song): song 
                            for song in songs_to_process
                        }
                        
                        for future in as_completed(future_to_song):
                            song = future_to_song[future]
                            success, message = future.result()
                            
                            if success:
                                batch_completed.append(song.song_id)
                            else:
                                batch_failed.append(song.song_id)
                                self.logger.warning(message)
                            
                            total_processed += 1
                            
                            if total_processed % 10 == 0:
                                self.logger.info(f"Progress: {total_processed} songs processed")
                                self.logger.info(f"Stats - API calls: {self.stats['api_calls']}, "
                                               f"Cache hits: {self.stats['cache_hits']}, "
                                               f"Success rate: {self.stats['processed']/(self.stats['processed']+self.stats['failures'])*100:.1f}%")
                    
                    # Save checkpoint after each batch
                    self.save_checkpoint(current_id + batch_size, current_id, batch_completed, batch_failed)
                
                current_id += batch_size
                
                # Periodic saves
                if total_processed % 50 == 0:
                    self.save_cache()
                    self.save_failed_songs()
                
                # Small delay between batches
                time.sleep(0.5)
            
            # Final saves
            self.save_cache()
            self.save_failed_songs()
            
            self.logger.info(f"Processing completed! Final stats: {self.stats}")
            
        except Exception as e:
            self.logger.error(f"Error in batch processing: {e}")
            self.save_cache()
            self.save_failed_songs()
            raise
        finally:
            session.close()

# Configure logging
def setup_logging(process_id):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # File handler
    fh = logging.FileHandler(f'spotify_optimized_{process_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    fh.setLevel(logging.INFO)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)

def main():
    parser = argparse.ArgumentParser(description="Optimized Spotify genre processing")
    parser.add_argument("--min_id", type=int, required=True)
    parser.add_argument("--max_id", type=int, required=True)
    parser.add_argument("--process_id", type=int, default=0)
    parser.add_argument("--max_workers", type=int, default=2)
    parser.add_argument("--batch_size", type=int, default=25)
    
    args = parser.parse_args()
    
    setup_logging(args.process_id)
    logger = logging.getLogger("main")
    
    logger.info(f"Starting optimized Spotify genre processing:")
    logger.info(f"  ID range: {args.min_id} - {args.max_id}")
    logger.info(f"  Process ID: {args.process_id}")
    logger.info(f"  Max workers: {args.max_workers}")
    logger.info(f"  Batch size: {args.batch_size}")
    
    processor = OptimizedSpotifyProcessor(
        process_id=args.process_id,
        max_workers=args.max_workers
    )
    
    try:
        processor.process_batch_optimized(args.min_id, args.max_id, args.batch_size)
        logger.info("Processing completed successfully!")
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        processor.save_cache()
        processor.save_failed_songs()
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    main()