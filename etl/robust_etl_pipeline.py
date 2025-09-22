#!/usr/bin/env python3
"""
TracktionAI Robust ETL Pipeline
==============================
Production-ready ETL pipeline with comprehensive error handling,
data validation, chunked processing, and synthetic data generation.
"""

import pandas as pd
import numpy as np
import json
import os
import sys
from datetime import datetime, date
import random
from collections import defaultdict, Counter
import logging
import time
from typing import Dict, List, Any, Tuple, Optional, Union
import gc
import warnings
from pathlib import Path
import traceback
warnings.filterwarnings('ignore')

from analytics_generator import AnalyticsGenerator

# Configure logging
def setup_logging(log_file: str = 'etl_robust.log') -> logging.Logger:
    """Setup comprehensive logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

class DataValidator:
    """Data validation utilities"""
    
    @staticmethod
    def validate_listen_event(record: Dict) -> Tuple[bool, str]:
        """Validate listen event record"""
        required_fields = ['userId', 'artist', 'song']
        for field in required_fields:
            if not record.get(field):
                return False, f"Missing required field: {field}"
        
        # Additional validation
        if record.get('length', 0) < 0:
            return False, "Invalid song length"
        
        return True, "Valid"
    
    @staticmethod
    def validate_auth_event(record: Dict) -> Tuple[bool, str]:
        """Validate auth event record"""
        required_fields = ['userId']
        for field in required_fields:
            if not record.get(field):
                return False, f"Missing required field: {field}"
        return True, "Valid"
    
    @staticmethod
    def validate_page_view_event(record: Dict) -> Tuple[bool, str]:
        """Validate page view event record"""
        required_fields = ['userId', 'page']
        for field in required_fields:
            if not record.get(field):
                return False, f"Missing required field: {field}"
        return True, "Valid"
    
    @staticmethod
    def validate_status_change_event(record: Dict) -> Tuple[bool, str]:
        """Validate status change event record"""
        required_fields = ['userId']
        for field in required_fields:
            if not record.get(field):
                return False, f"Missing required field: {field}"
        return True, "Valid"

class SyntheticDataGenerator:
    """Generate high-quality synthetic data"""
    
    def __init__(self, seed: int = 42):
        np.random.seed(seed)
        random.seed(seed)
        
        # Load genre mappings for sophisticated genre generation
        self.genre_mappings = self._load_genre_mappings()
        
    def _load_genre_mappings(self) -> Dict[str, List[str]]:
        """Load sophisticated genre mappings"""
        return {
            'rock': ['alternative', 'indie', 'classic rock', 'hard rock', 'punk', 'grunge'],
            'pop': ['pop', 'dance pop', 'electropop', 'indie pop', 'synthpop'],
            'hip_hop': ['hip hop', 'rap', 'trap', 'conscious rap', 'gangsta rap'],
            'electronic': ['edm', 'house', 'techno', 'dubstep', 'ambient', 'chillout'],
            'r_and_b': ['r&b', 'soul', 'neo soul', 'contemporary r&b'],
            'country': ['country', 'country rock', 'bluegrass', 'folk country'],
            'jazz': ['jazz', 'smooth jazz', 'bebop', 'fusion', 'swing'],
            'classical': ['classical', 'orchestral', 'chamber music', 'opera'],
            'latin': ['reggaeton', 'salsa', 'bachata', 'latin pop', 'mariachi'],
            'metal': ['heavy metal', 'death metal', 'black metal', 'progressive metal'],
            'folk': ['folk', 'indie folk', 'acoustic', 'singer-songwriter'],
            'blues': ['blues', 'electric blues', 'delta blues', 'chicago blues']
        }
    
    def generate_realistic_age(self) -> int:
        """
        Generate realistic age distribution focused on younger demographics
        40% Gen Z (13-24), 30% Millennials (25-34), 18% Gen X (35-44), etc.
        """
        age_ranges = [
            (13, 24, 0.40),  # 40% Gen Z younger
            (25, 34, 0.30),  # 30% Millennials  
            (35, 44, 0.18),  # 18% Older Millennials
            (45, 54, 0.08),  # 8% Gen X
            (55, 64, 0.03),  # 3% Younger Boomers
            (65, 79, 0.01)   # 1% Older demographics
        ]
        
        range_choice = np.random.choice(len(age_ranges), p=[r[2] for r in age_ranges])
        min_age, max_age, _ = age_ranges[range_choice]
        return np.random.randint(min_age, max_age + 1)
    
    def generate_sophisticated_genre(self, artist_name: str, song_title: str = "") -> str:
        """Generate realistic genres using artist/song analysis"""
        artist_lower = artist_name.lower()
        song_lower = song_title.lower()
        
        # Enhanced keyword-based genre detection
        genre_keywords = {
            'rock': ['rock', 'stone', 'metal', 'fire', 'rebel', 'wild', 'storm', 'thunder'],
            'pop': ['pop', 'star', 'dream', 'shine', 'bright', 'love', 'heart', 'sweet'],
            'hip_hop': ['rap', 'hip', 'hop', 'street', 'urban', 'flow', 'beat', 'gang'],
            'electronic': ['electronic', 'digital', 'cyber', 'synth', 'tech', 'electric', 'wave'],
            'r_and_b': ['soul', 'smooth', 'silk', 'velvet', 'honey', 'chocolate', 'brown'],
            'country': ['country', 'cowboy', 'southern', 'farm', 'road', 'home', 'whiskey'],
            'jazz': ['jazz', 'blue', 'mood', 'night', 'lounge', 'smooth', 'cafe'],
            'latin': ['latin', 'spanish', 'fiesta', 'caliente', 'salsa', 'mambo', 'tango'],
            'folk': ['folk', 'acoustic', 'simple', 'story', 'mountain', 'river', 'wind'],
            'classical': ['classical', 'symphony', 'orchestra', 'piano', 'violin', 'opera']
        }
        
        # Score each genre based on keywords
        genre_scores = {}
        for genre, keywords in genre_keywords.items():
            score = sum(1 for keyword in keywords if keyword in artist_lower or keyword in song_lower)
            if score > 0:
                genre_scores[genre] = score
        
        # If we found keyword matches, choose weighted by score
        if genre_scores:
            max_score = max(genre_scores.values())
            top_genres = [g for g, s in genre_scores.items() if s == max_score]
            chosen_genre = random.choice(top_genres)
            return random.choice(self.genre_mappings[chosen_genre])
        
        # Fallback: weighted random selection based on popularity
        genre_weights = {
            'pop': 0.25, 'rock': 0.20, 'hip_hop': 0.18, 'electronic': 0.12,
            'r_and_b': 0.08, 'country': 0.06, 'latin': 0.04, 'jazz': 0.03,
            'folk': 0.02, 'classical': 0.02
        }
        
        chosen_genre = np.random.choice(list(genre_weights.keys()), p=list(genre_weights.values()))
        return random.choice(self.genre_mappings[chosen_genre])

class RobustETLPipeline:
    """Production-ready ETL Pipeline with comprehensive error handling"""
    
    def __init__(self, chunk_size: int = 50000, data_dir: str = "data"):
        """Initialize robust ETL pipeline"""
        self.chunk_size = chunk_size
        self.data_dir = Path(data_dir)
        
        # Initialize components
        self.validator = DataValidator()
        self.synthetic_data = SyntheticDataGenerator()
        self.analytics = AnalyticsGenerator()
        
        # Statistics tracking
        self.processing_stats = {
            'total_records': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'errors': defaultdict(int)
        }
        
        # Data containers
        self.processed_results = {
            'listen': {'chunk_results': [], 'summary': {}},
            'auth': {'chunk_results': [], 'summary': {}},
            'page_views': {'chunk_results': [], 'summary': {}},
            'status_changes': {'chunk_results': [], 'summary': {}}
        }
        
        logger.info(f"🚀 Robust ETL Pipeline initialized (chunk_size: {chunk_size:,})")
    
    def safe_file_reader(self, file_path: Path, chunk_size: Optional[int] = None):
        """Safely read JSONL files with error handling"""
        if chunk_size is None:
            chunk_size = self.chunk_size
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                chunk = []
                for line_num, line in enumerate(f, 1):
                    try:
                        line = line.strip()
                        if not line:
                            continue
                        
                        record = json.loads(line)
                        chunk.append(record)
                        
                        if len(chunk) >= chunk_size:
                            yield chunk
                            chunk = []
                            
                    except json.JSONDecodeError as e:
                        self.processing_stats['errors']['json_decode'] += 1
                        logger.warning(f"JSON decode error at line {line_num} in {file_path}: {e}")
                        continue
                    except Exception as e:
                        self.processing_stats['errors']['unexpected'] += 1
                        logger.warning(f"Unexpected error at line {line_num} in {file_path}: {e}")
                        continue
                
                # Yield remaining records
                if chunk:
                    yield chunk
                    
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            raise
    
    def process_listen_events_chunk(self, chunk_data: List[Dict], chunk_num: int) -> Dict[str, Any]:
        """Process listen events chunk with comprehensive validation"""
        chunk_results = {
            'listen_events': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'unique_users': set(),
            'unique_artists': set(),
            'unique_songs': set(),
            'genres_generated': defaultdict(int),
            'user_ages': {},
            'geographic_data': defaultdict(lambda: defaultdict(int)),
            'temporal_data': defaultdict(int),
            'session_data': defaultdict(list),
            'engagement_data': defaultdict(lambda: {
                'total_plays': 0,
                'total_duration': 0,
                'unique_users': set(),
                'total_sessions': set()
            }),
            'artist_popularity': defaultdict(int),
            'song_popularity': defaultdict(int),
            'duration_stats': []
        }
        
        for record_idx, record in enumerate(chunk_data):
            try:
                chunk_results['listen_events'] += 1
                
                # Validate record
                is_valid, validation_msg = self.validator.validate_listen_event(record)
                if not is_valid:
                    chunk_results['invalid_records'] += 1
                    self.processing_stats['errors']['validation'] += 1
                    continue
                
                chunk_results['valid_records'] += 1
                
                # Extract fields with safe defaults
                user_id = str(record.get('userId', ''))
                artist = str(record.get('artist', '')).strip()
                song = str(record.get('song', '')).strip()
                duration = max(0, int(record.get('length', 0)))
                ts = int(record.get('ts', 0))
                session_id = str(record.get('sessionId', ''))
                level = str(record.get('level', 'free')).lower()
                city = str(record.get('city', 'Unknown')).strip()
                state = str(record.get('state', 'Unknown')).strip()
                
                # Generate synthetic data
                if user_id and user_id not in chunk_results['user_ages']:
                    chunk_results['user_ages'][user_id] = self.synthetic_data.generate_realistic_age()
                
                if artist and song:
                    genre = self.synthetic_data.generate_sophisticated_genre(artist, song)
                    chunk_results['genres_generated'][genre] += 1
                
                # Update tracking data
                if user_id:
                    chunk_results['unique_users'].add(user_id)
                if artist:
                    chunk_results['unique_artists'].add(artist)
                    chunk_results['artist_popularity'][artist] += 1
                if artist and song:
                    song_key = f"{artist}::{song}"
                    chunk_results['unique_songs'].add(song_key)
                    chunk_results['song_popularity'][song_key] += 1
                
                # Geographic analysis
                if state != 'Unknown' and city != 'Unknown':
                    chunk_results['geographic_data'][state][city] += 1
                
                # Temporal analysis
                if ts > 0:
                    try:
                        dt = datetime.fromtimestamp(ts / 1000)
                        chunk_results['temporal_data'][dt.hour] += 1
                        chunk_results['temporal_data'][f"date_{dt.date()}"] += 1
                        chunk_results['temporal_data'][f"weekday_{dt.weekday()}"] += 1
                    except (ValueError, OSError):
                        logger.warning(f"Invalid timestamp: {ts}")
                
                # Session analysis
                if session_id and user_id:
                    chunk_results['session_data'][session_id].append({
                        'user_id': user_id,
                        'artist': artist,
                        'song': song,
                        'duration': duration,
                        'timestamp': ts
                    })
                
                # Engagement analysis by subscription level
                if level in ['free', 'paid']:
                    eng_data = chunk_results['engagement_data'][level]
                    eng_data['total_plays'] += 1
                    eng_data['total_duration'] += duration
                    if user_id:
                        eng_data['unique_users'].add(user_id)
                    if session_id:
                        eng_data['total_sessions'].add(session_id)
                
                # Duration statistics
                if duration > 0:
                    chunk_results['duration_stats'].append(duration)
                
            except Exception as e:
                chunk_results['invalid_records'] += 1
                self.processing_stats['errors']['processing'] += 1
                logger.warning(f"Error processing listen event {record_idx} in chunk {chunk_num}: {e}")
                continue
        
        return chunk_results
    
    def process_auth_events_chunk(self, chunk_data: List[Dict], chunk_num: int) -> Dict[str, Any]:
        """Process auth events chunk"""
        chunk_results = {
            'auth_events': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'unique_users': set(),
            'success_logins': 0,
            'failed_logins': 0,
            'registrations': 0,
            'temporal_patterns': defaultdict(int),
            'geographic_patterns': defaultdict(int)
        }
        
        for record_idx, record in enumerate(chunk_data):
            try:
                chunk_results['auth_events'] += 1
                
                # Validate record
                is_valid, validation_msg = self.validator.validate_auth_event(record)
                if not is_valid:
                    chunk_results['invalid_records'] += 1
                    continue
                
                chunk_results['valid_records'] += 1
                
                # Extract fields
                user_id = str(record.get('userId', ''))
                success = record.get('success', True)
                method = str(record.get('method', 'unknown')).lower()
                ts = int(record.get('ts', 0))
                city = str(record.get('city', 'Unknown'))
                state = str(record.get('state', 'Unknown'))
                
                if user_id:
                    chunk_results['unique_users'].add(user_id)
                
                # Categorize auth events
                if method == 'put' and success:
                    chunk_results['success_logins'] += 1
                elif method == 'put' and not success:
                    chunk_results['failed_logins'] += 1
                elif method == 'post':
                    chunk_results['registrations'] += 1
                
                # Temporal patterns
                if ts > 0:
                    try:
                        dt = datetime.fromtimestamp(ts / 1000)
                        chunk_results['temporal_patterns'][dt.hour] += 1
                    except (ValueError, OSError):
                        pass
                
                # Geographic patterns
                if state != 'Unknown':
                    chunk_results['geographic_patterns'][state] += 1
                
            except Exception as e:
                chunk_results['invalid_records'] += 1
                logger.warning(f"Error processing auth event {record_idx} in chunk {chunk_num}: {e}")
                continue
        
        return chunk_results
    
    def process_page_view_events_chunk(self, chunk_data: List[Dict], chunk_num: int) -> Dict[str, Any]:
        """Process page view events chunk"""
        chunk_results = {
            'page_view_events': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'unique_users': set(),
            'page_popularity': defaultdict(int),
            'temporal_patterns': defaultdict(int),
            'user_journeys': defaultdict(list)
        }
        
        for record_idx, record in enumerate(chunk_data):
            try:
                chunk_results['page_view_events'] += 1
                
                # Validate record
                is_valid, validation_msg = self.validator.validate_page_view_event(record)
                if not is_valid:
                    chunk_results['invalid_records'] += 1
                    continue
                
                chunk_results['valid_records'] += 1
                
                # Extract fields
                user_id = str(record.get('userId', ''))
                page = str(record.get('page', '')).strip()
                ts = int(record.get('ts', 0))
                session_id = str(record.get('sessionId', ''))
                
                if user_id:
                    chunk_results['unique_users'].add(user_id)
                
                if page:
                    chunk_results['page_popularity'][page] += 1
                
                # Temporal patterns
                if ts > 0:
                    try:
                        dt = datetime.fromtimestamp(ts / 1000)
                        chunk_results['temporal_patterns'][dt.hour] += 1
                    except (ValueError, OSError):
                        pass
                
                # User journey tracking
                if user_id and page and ts > 0:
                    chunk_results['user_journeys'][user_id].append({
                        'page': page,
                        'timestamp': ts,
                        'session_id': session_id
                    })
                
            except Exception as e:
                chunk_results['invalid_records'] += 1
                logger.warning(f"Error processing page view event {record_idx} in chunk {chunk_num}: {e}")
                continue
        
        return chunk_results
    
    def process_status_change_events_chunk(self, chunk_data: List[Dict], chunk_num: int) -> Dict[str, Any]:
        """Process status change events chunk"""
        chunk_results = {
            'status_change_events': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'unique_users': set(),
            'upgrades': 0,
            'downgrades': 0,
            'cancellations': 0,
            'temporal_patterns': defaultdict(int)
        }
        
        for record_idx, record in enumerate(chunk_data):
            try:
                chunk_results['status_change_events'] += 1
                
                # Validate record
                is_valid, validation_msg = self.validator.validate_status_change_event(record)
                if not is_valid:
                    chunk_results['invalid_records'] += 1
                    continue
                
                chunk_results['valid_records'] += 1
                
                # Extract fields
                user_id = str(record.get('userId', ''))
                level = str(record.get('level', '')).lower()
                ts = int(record.get('ts', 0))
                
                if user_id:
                    chunk_results['unique_users'].add(user_id)
                
                # Categorize status changes
                if 'paid' in level:
                    chunk_results['upgrades'] += 1
                elif 'free' in level:
                    chunk_results['downgrades'] += 1
                elif 'cancel' in level:
                    chunk_results['cancellations'] += 1
                
                # Temporal patterns
                if ts > 0:
                    try:
                        dt = datetime.fromtimestamp(ts / 1000)
                        chunk_results['temporal_patterns'][dt.hour] += 1
                    except (ValueError, OSError):
                        pass
                
            except Exception as e:
                chunk_results['invalid_records'] += 1
                logger.warning(f"Error processing status change event {record_idx} in chunk {chunk_num}: {e}")
                continue
        
        return chunk_results
    
    def process_file_in_chunks(self, file_path: Path, file_type: str) -> Dict[str, Any]:
        """Process a file in chunks with comprehensive error handling"""
        logger.info(f"🔄 Processing {file_type} file: {file_path}")
        
        # Select appropriate processing function
        process_functions = {
            'listen': self.process_listen_events_chunk,
            'auth': self.process_auth_events_chunk,
            'page_views': self.process_page_view_events_chunk,
            'status_changes': self.process_status_change_events_chunk
        }
        
        process_func = process_functions.get(file_type)
        if not process_func:
            raise ValueError(f"Unknown file type: {file_type}")
        
        results = {
            'total_records': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'processed_chunks': 0,
            'processing_time': 0.0
        }
        
        start_time = time.time()
        
        try:
            for chunk_num, chunk_data in enumerate(self.safe_file_reader(file_path), 1):
                logger.info(f"   📊 Processing chunk {chunk_num} ({len(chunk_data):,} records)")
                
                chunk_results = process_func(chunk_data, chunk_num)
                
                # Update overall statistics
                results['total_records'] += chunk_results.get('listen_events', 0) or \
                                          chunk_results.get('auth_events', 0) or \
                                          chunk_results.get('page_view_events', 0) or \
                                          chunk_results.get('status_change_events', 0)
                results['valid_records'] += chunk_results.get('valid_records', 0)
                results['invalid_records'] += chunk_results.get('invalid_records', 0)
                results['processed_chunks'] += 1
                
                # Store chunk results
                self.processed_results[file_type]['chunk_results'].append(chunk_results)
                
                # Memory management
                if chunk_num % 10 == 0:
                    gc.collect()
            
            results['processing_time'] = time.time() - start_time
            
            logger.info(f"   ✅ Completed {file_type}: {results['total_records']:,} total records "
                       f"({results['valid_records']:,} valid, {results['invalid_records']:,} invalid) "
                       f"in {results['processed_chunks']} chunks ({results['processing_time']:.1f}s)")
            
            return results
            
        except Exception as e:
            logger.error(f"   ❌ Error processing {file_type}: {e}")
            logger.error(traceback.format_exc())
            raise
    
    def run_pipeline(self) -> Dict[str, Any]:
        """Run the complete ETL pipeline"""
        logger.info("🎯 Starting Robust ETL Pipeline")
        
        # File mappings
        file_mappings = {
            'listen': 'events_small_combined.jsonl',
            'auth': 'auth_events_head.jsonl',
            'page_views': 'page_view_events_head.jsonl',
            'status_changes': 'status_change_events_head.jsonl'
        }
        
        pipeline_start = time.time()
        overall_results = {}
        
        try:
            # Process each file type
            for file_type, filename in file_mappings.items():
                file_path = self.data_dir / filename
                
                if not file_path.exists():
                    logger.warning(f"⚠️ File not found: {file_path}")
                    continue
                
                file_results = self.process_file_in_chunks(file_path, file_type)
                overall_results[file_type] = file_results
            
            # Generate analytics
            logger.info("📈 Generating comprehensive analytics...")
            analytics_results = self.analytics.generate_all_analytics(self.processed_results)
            
            # Create summary
            total_time = time.time() - pipeline_start
            pipeline_summary = {
                'processing_time': total_time,
                'total_records_processed': sum(r.get('total_records', 0) for r in overall_results.values()),
                'total_valid_records': sum(r.get('valid_records', 0) for r in overall_results.values()),
                'total_invalid_records': sum(r.get('invalid_records', 0) for r in overall_results.values()),
                'files_processed': len(overall_results),
                'analytics_generated': len(analytics_results),
                'processing_stats': dict(self.processing_stats)
            }
            
            logger.info(f"🎉 ETL Pipeline completed successfully!")
            logger.info(f"   ⏱️ Total processing time: {total_time:.1f} seconds")
            logger.info(f"   📊 Records processed: {pipeline_summary['total_records_processed']:,}")
            logger.info(f"   ✅ Valid records: {pipeline_summary['total_valid_records']:,}")
            logger.info(f"   ❌ Invalid records: {pipeline_summary['total_invalid_records']:,}")
            logger.info(f"   📁 Files processed: {pipeline_summary['files_processed']}")
            logger.info(f"   📈 Analytics files generated: {pipeline_summary['analytics_generated']}")
            
            return {
                'success': True,
                'pipeline_summary': pipeline_summary,
                'file_results': overall_results,
                'analytics_results': analytics_results
            }
            
        except Exception as e:
            logger.error(f"💥 ETL Pipeline failed: {e}")
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': str(e),
                'partial_results': overall_results
            }

def main():
    """Main execution function"""
    try:
        # Initialize pipeline
        etl = RobustETLPipeline(chunk_size=50000, data_dir="sample")
        
        # Run pipeline
        results = etl.run_pipeline()
        
        if results['success']:
            logger.info("🚀 ETL Pipeline execution completed successfully!")
            return 0
        else:
            logger.error("💥 ETL Pipeline failed!")
            return 1
            
    except KeyboardInterrupt:
        logger.info("⏹️ ETL Pipeline interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())
