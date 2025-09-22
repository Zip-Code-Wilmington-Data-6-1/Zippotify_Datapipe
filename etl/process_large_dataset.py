#!/usr/bin/env python3
"""
TracktionAI Large Dataset ETL Pipeline
=====================================
Processes 11GB static dataset with chunked processing, synthetic data generation,
and star schema compliance. Generates all aggregated analytics files.
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
from typing import Dict, List, Any, Tuple
import gc
import warnings
warnings.filterwarnings('ignore')

from analytics_generator import AnalyticsGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_processing.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class TracktionAIETL:
    """ETL Pipeline for processing large TracktionAI dataset"""
    
    def __init__(self, chunk_size: int = 50000):
        """
        Initialize ETL pipeline
        
        Args:
            chunk_size: Number of records to process at once (memory management)
        """
        self.chunk_size = chunk_size
        self.processed_stats = {
            'users': set(),
            'artists': set(), 
            'songs': set(),
            'genres': set(),
            'locations': set()
        }
        
        # Set random seeds for reproducible results
        np.random.seed(42)
        random.seed(42)
        
        # Initialize data containers
        self.aggregated_data = {}
        self.csv_outputs = {}
        self.processed_chunk_results = {
            'listen': {'chunk_results': []},
            'auth': {'chunk_results': []},
            'page_views': {'chunk_results': []},
            'status_changes': {'chunk_results': []}
        }
        
        logger.info(f"🚀 TracktionAI ETL Pipeline initialized with chunk size: {chunk_size:,}")

    def generate_realistic_age(self) -> int:
        """
        Generate realistic age distribution focused on younger demographics
        
        Returns:
            int: Age between 13-79
        """
        age_ranges = [
            (13, 24, 0.40),  # 40% Gen Z younger (13-24)
            (25, 34, 0.30),  # 30% Millennials (25-34) 
            (35, 44, 0.18),  # 18% Older Millennials (35-44)
            (45, 54, 0.08),  # 8% Gen X (45-54)
            (55, 64, 0.03),  # 3% Younger Boomers (55-64)
            (65, 79, 0.01)   # 1% Older demographics (65-79)
        ]
        
        range_choice = np.random.choice(len(age_ranges), p=[r[2] for r in age_ranges])
        min_age, max_age, _ = age_ranges[range_choice]
        return np.random.randint(min_age, max_age + 1)

    def generate_sophisticated_genre(self, artist_name: str, song_title: str = "") -> str:
        """
        Generate realistic genres using sophisticated artist/song analysis
        
        Args:
            artist_name: Name of the artist
            song_title: Title of the song (optional for additional context)
            
        Returns:
            str: Genre classification
        """
        # Comprehensive genre mapping with real artist patterns
        genre_patterns = {
            'pop': {
                'artists': ['justin', 'taylor', 'ariana', 'ed sheeran', 'billie', 'dua', 'weeknd', 
                           'bruno mars', 'lady gaga', 'katy perry', 'selena', 'miley', 'pink',
                           'britney', 'christina', 'madonna', 'maroon 5', 'onerepublic'],
                'keywords': ['pop', 'mainstream', 'radio', 'hit', 'single', 'love', 'heart'],
                'weight': 0.22
            },
            'rock': {
                'artists': ['queen', 'beatles', 'led zeppelin', 'pink floyd', 'foo fighters', 
                           'red hot', 'green day', 'linkin park', 'coldplay', 'u2', 'radiohead',
                           'pearl jam', 'nirvana', 'metallica', 'ac/dc', 'guns', 'rolling stones'],
                'keywords': ['rock', 'guitar', 'band', 'live', 'concert', 'electric'],
                'weight': 0.18
            },
            'hip-hop': {
                'artists': ['drake', 'kendrick', 'jay-z', 'kanye', 'eminem', 'nas', 'future',
                           'travis scott', 'post malone', 'lil', 'big', 'notorious', 'tupac',
                           'snoop', 'dr. dre', 'chance', 'childish', 'tyler'],
                'keywords': ['rap', 'hip', 'hop', 'beats', 'flow', 'bars', 'remix'],
                'weight': 0.15
            },
            'r&b': {
                'artists': ['beyoncé', 'alicia keys', 'john legend', 'usher', 'rihanna',
                           'chris brown', 'sza', 'the weeknd', 'frank ocean', 'miguel',
                           'janelle', 'solange', 'h.e.r.', 'daniel caesar'],
                'keywords': ['soul', 'smooth', 'vocals', 'love', 'groove', 'sultry'],
                'weight': 0.12
            },
            'electronic': {
                'artists': ['skrillex', 'calvin harris', 'david guetta', 'deadmau5', 'daft punk',
                           'tiësto', 'avicii', 'swedish house', 'diplo', 'flume', 'odesza',
                           'porter robinson', 'madeon', 'zedd', 'marshmello'],
                'keywords': ['electronic', 'edm', 'dance', 'remix', 'mix', 'beat', 'synth'],
                'weight': 0.10
            },
            'indie': {
                'artists': ['arctic monkeys', 'vampire weekend', 'foster people', 'mumford',
                           'of monsters', 'bon iver', 'tame impala', 'alt-j', 'glass animals',
                           'the strokes', 'yeah yeah yeahs', 'interpol', 'modest mouse'],
                'keywords': ['indie', 'alternative', 'underground', 'art', 'experimental'],
                'weight': 0.08
            },
            'country': {
                'artists': ['luke bryan', 'keith urban', 'carrie underwood', 'blake shelton',
                           'miranda lambert', 'chris stapleton', 'kacey musgraves', 'maren morris',
                           'florida georgia', 'little big town', 'keith whitley'],
                'keywords': ['country', 'nashville', 'southern', 'folk', 'acoustic', 'guitar'],
                'weight': 0.06
            },
            'latin': {
                'artists': ['shakira', 'manu chao', 'jesse joy', 'mana', 'julieta venegas',
                           'bad bunny', 'j balvin', 'ozuna', 'maluma', 'daddy yankee',
                           'luis fonsi', 'enrique iglesias', 'ricky martin'],
                'keywords': ['latin', 'spanish', 'reggaeton', 'salsa', 'bachata', 'merengue'],
                'weight': 0.05
            },
            'jazz': {
                'artists': ['miles davis', 'john coltrane', 'ella fitzgerald', 'louis armstrong',
                           'duke ellington', 'charlie parker', 'billie holiday', 'nina simone',
                           'herbie hancock', 'wynton marsalis'],
                'keywords': ['jazz', 'swing', 'blues', 'improvisation', 'saxophone', 'piano'],
                'weight': 0.02
            },
            'classical': {
                'artists': ['mozart', 'bach', 'beethoven', 'chopin', 'ludovico einaudi',
                           'max richter', 'ólafur arnalds', 'nils frahm', 'yann tiersen'],
                'keywords': ['classical', 'symphony', 'concerto', 'orchestra', 'piano', 'strings'],
                'weight': 0.02
            }
        }
        
        artist_lower = artist_name.lower()
        song_lower = song_title.lower()
        
        # Score each genre based on artist and song matches
        genre_scores = {}
        for genre, data in genre_patterns.items():
            score = 0
            
            # Check artist name matches
            for artist_keyword in data['artists']:
                if artist_keyword in artist_lower:
                    score += 10
                    break
            
            # Check song title matches (bonus points)
            for song_keyword in data['keywords']:
                if song_keyword in song_lower:
                    score += 5
                if song_keyword in artist_lower:
                    score += 3
            
            genre_scores[genre] = score
        
        # If we have a clear match, use it
        if max(genre_scores.values()) > 0:
            best_genre = max(genre_scores.items(), key=lambda x: x[1])[0]
            return best_genre
        
        # Otherwise, use weighted random distribution
        genres = list(genre_patterns.keys())
        weights = [genre_patterns[g]['weight'] for g in genres]
        return np.random.choice(genres, p=weights)

    def process_file_in_chunks(self, file_path: str, process_func, file_type: str) -> Dict[str, Any]:
        """
        Process large files in chunks to manage memory
        
        Args:
            file_path: Path to the data file
            process_func: Function to process each chunk
            file_type: Type of file being processed
            
        Returns:
            Dict with processing results
        """
        logger.info(f"📂 Processing {file_type} file: {os.path.basename(file_path)}")
        
        results = {
            'total_records': 0,
            'processed_chunks': 0,
            'data_summary': defaultdict(int)
        }
        
        try:
            chunk_count = 0
            with open(file_path, 'r', encoding='utf-8') as file:
                chunk_data = []
                
                for line_num, line in enumerate(file, 1):
                    try:
                        record = json.loads(line.strip())
                        chunk_data.append(record)
                        
                        if len(chunk_data) >= self.chunk_size:
                            # Process chunk
                            chunk_results = process_func(chunk_data, chunk_count)
                            
                            # Update results
                            results['total_records'] += len(chunk_data)
                            results['processed_chunks'] += 1
                            
                            # Merge chunk results safely
                            for key, value in chunk_results.items():
                                if isinstance(value, (int, float)):
                                    results['data_summary'][key] += value
                                elif isinstance(value, set):
                                    results['data_summary'][f"{key}_count"] = len(value)
                                elif isinstance(value, dict):
                                    # Handle complex nested structures carefully
                                    if key == 'engagement_data':
                                        # Skip merging engagement_data to avoid corruption
                                        continue
                                    else:
                                        for subkey, subvalue in value.items():
                                            if isinstance(subvalue, (int, float)):
                                                results['data_summary'][f"{key}_{subkey}"] += subvalue
                                            elif isinstance(subvalue, set):
                                                results['data_summary'][f"{key}_{subkey}_count"] = len(subvalue)
                            
                            # Store chunk results for detailed analytics
                            if file_type not in self.processed_chunk_results:
                                self.processed_chunk_results[file_type] = {'chunk_results': []}
                            self.processed_chunk_results[file_type]['chunk_results'].append(chunk_results)
                            
                            # Clear chunk and force garbage collection
                            chunk_data = []
                            chunk_count += 1
                            gc.collect()
                            
                            # Progress logging
                            if chunk_count % 10 == 0:
                                logger.info(f"   ✓ Processed {chunk_count} chunks ({results['total_records']:,} records)")
                    
                    except json.JSONDecodeError as e:
                        logger.warning(f"   ⚠️ Skipped invalid JSON at line {line_num}: {e}")
                        continue
                
                # Process remaining data
                if chunk_data:
                    chunk_results = process_func(chunk_data, chunk_count)
                    results['total_records'] += len(chunk_data)
                    results['processed_chunks'] += 1
                    
                    for key, value in chunk_results.items():
                        if isinstance(value, (int, float)):
                            results['data_summary'][key] += value
                        elif isinstance(value, set):
                            results['data_summary'][f"{key}_count"] = len(value)
                        elif isinstance(value, dict):
                            # Handle complex nested structures carefully
                            if key == 'engagement_data':
                                # Skip merging engagement_data to avoid corruption
                                continue
                            else:
                                for subkey, subvalue in value.items():
                                    if isinstance(subvalue, (int, float)):
                                        results['data_summary'][f"{key}_{subkey}"] += subvalue
                                    elif isinstance(subvalue, set):
                                        results['data_summary'][f"{key}_{subkey}_count"] = len(subvalue)
                    
                    # Store final chunk results
                    if file_type not in self.processed_chunk_results:
                        self.processed_chunk_results[file_type] = {'chunk_results': []}
                    self.processed_chunk_results[file_type]['chunk_results'].append(chunk_results)
            
            logger.info(f"   ✅ Completed {file_type}: {results['total_records']:,} records in {results['processed_chunks']} chunks")
            return results
            
        except Exception as e:
            logger.error(f"   ❌ Error processing {file_type}: {e}")
            raise

    def process_listen_events_chunk(self, chunk_data: List[Dict], chunk_num: int) -> Dict[str, Any]:
        """Process a chunk of listen events data"""
        chunk_results = {
            'listen_events': len(chunk_data),
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
                'unique_users': set()
            })
        }
        
        for record in chunk_data:
            try:
                # Extract and validate required fields
                user_id = record.get('userId')
                artist = record.get('artist', 'Unknown Artist')
                song = record.get('song', 'Unknown Song')
                duration = float(record.get('duration', 0))
                ts = int(record.get('ts', 0))
                session_id = record.get('sessionId')
                level = record.get('level', 'free')
                city = record.get('city', 'Unknown')
                state = record.get('state', 'Unknown')
                
                if not user_id or not artist or not song:
                    continue
                
                # Generate synthetic data
                if user_id not in chunk_results['user_ages']:
                    chunk_results['user_ages'][user_id] = self.generate_realistic_age()
                
                genre = self.generate_sophisticated_genre(artist, song)
                
                # Update tracking sets
                chunk_results['unique_users'].add(user_id)
                chunk_results['unique_artists'].add(artist)
                chunk_results['unique_songs'].add(f"{artist}::{song}")
                chunk_results['genres_generated'][genre] += 1
                
                # Geographic analysis
                if state != 'Unknown' and city != 'Unknown':
                    chunk_results['geographic_data'][state][city] += 1
                
                # Temporal analysis
                if ts > 0:
                    dt = datetime.fromtimestamp(ts / 1000)
                    chunk_results['temporal_data'][dt.hour] += 1
                    chunk_results['temporal_data'][f"date_{dt.date()}"] += 1
                
                # Session analysis
                if session_id:
                    chunk_results['session_data'][session_id].append({
                        'user_id': user_id,
                        'artist': artist,
                        'song': song,
                        'duration': duration,
                        'genre': genre,
                        'timestamp': ts
                    })
                
                # Engagement analysis
                chunk_results['engagement_data'][level]['total_plays'] += 1
                chunk_results['engagement_data'][level]['total_duration'] += duration
                chunk_results['engagement_data'][level]['unique_users'].add(user_id)
                
            except Exception as e:
                logger.warning(f"   ⚠️ Error processing listen event in chunk {chunk_num}: {e}")
                continue
        
        return chunk_results

    def process_auth_events_chunk(self, chunk_data: List[Dict], chunk_num: int) -> Dict[str, Any]:
        """Process a chunk of auth events data"""
        chunk_results = {
            'auth_events': len(chunk_data),
            'successful_logins': 0,
            'failed_logins': 0,
            'user_details': {},
            'level_distribution': defaultdict(int)
        }
        
        for record in chunk_data:
            try:
                user_id = record.get('userId')
                success = record.get('success', False)
                level = record.get('level', 'free')
                first_name = record.get('firstName', '')
                last_name = record.get('lastName', '')
                gender = record.get('gender', 'Unknown')
                registration = record.get('registration', 0)
                
                if success:
                    chunk_results['successful_logins'] += 1
                else:
                    chunk_results['failed_logins'] += 1
                
                chunk_results['level_distribution'][level] += 1
                
                if user_id and first_name and last_name:
                    chunk_results['user_details'][user_id] = {
                        'first_name': first_name,
                        'last_name': last_name,
                        'gender': gender,
                        'level': level,
                        'registration': registration
                    }
                    
            except Exception as e:
                logger.warning(f"   ⚠️ Error processing auth event in chunk {chunk_num}: {e}")
                continue
        
        return chunk_results

    def process_page_view_events_chunk(self, chunk_data: List[Dict], chunk_num: int) -> Dict[str, Any]:
        """Process a chunk of page view events data"""
        chunk_results = {
            'page_views': len(chunk_data),
            'unique_users': set(),
            'page_types': defaultdict(int),
            'temporal_patterns': defaultdict(int)
        }
        
        for record in chunk_data:
            try:
                user_id = record.get('userId')
                page = record.get('page', 'Unknown')
                ts = int(record.get('ts', 0))
                
                if user_id:
                    chunk_results['unique_users'].add(user_id)
                
                chunk_results['page_types'][page] += 1
                
                if ts > 0:
                    dt = datetime.fromtimestamp(ts / 1000)
                    chunk_results['temporal_patterns'][dt.hour] += 1
                    
            except Exception as e:
                logger.warning(f"   ⚠️ Error processing page view event in chunk {chunk_num}: {e}")
                continue
        
        return chunk_results

    def process_status_change_events_chunk(self, chunk_data: List[Dict], chunk_num: int) -> Dict[str, Any]:
        """Process a chunk of status change events data"""
        chunk_results = {
            'status_changes': len(chunk_data),
            'unique_users': set(),
            'status_types': defaultdict(int)
        }
        
        for record in chunk_data:
            try:
                user_id = record.get('userId')
                status = record.get('status', 'Unknown')
                
                if user_id:
                    chunk_results['unique_users'].add(user_id)
                
                chunk_results['status_types'][status] += 1
                    
            except Exception as e:
                logger.warning(f"   ⚠️ Error processing status change event in chunk {chunk_num}: {e}")
                continue
        
        return chunk_results

    def run_etl_pipeline(self):
        """Run the complete ETL pipeline"""
        start_time = time.time()
        logger.info("🎯 Starting TracktionAI ETL Pipeline...")
        
        # Define file paths
        base_path = "/Users/iara/Projects/Zippotify_Datapipe/output"
        files = {
            'listen_events': os.path.join(base_path, 'listen_events'),
            'auth_events': os.path.join(base_path, 'auth_events'),
            'page_view_events': os.path.join(base_path, 'page_view_events'),
            'status_change_events': os.path.join(base_path, 'status_change_events')
        }
        
        # Verify all files exist
        for file_type, file_path in files.items():
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Required file not found: {file_path}")
        
        # Initialize aggregated results
        all_results = {}
        
        # Process each file type
        logger.info("📊 Phase 1: Processing raw data files...")
        
        # Process listen events (largest file, most important)
        all_results['listen'] = self.process_file_in_chunks(
            files['listen_events'], 
            self.process_listen_events_chunk, 
            'Listen Events'
        )
        
        # Process auth events
        all_results['auth'] = self.process_file_in_chunks(
            files['auth_events'], 
            self.process_auth_events_chunk, 
            'Auth Events'
        )
        
        # Process page view events
        all_results['page_views'] = self.process_file_in_chunks(
            files['page_view_events'], 
            self.process_page_view_events_chunk, 
            'Page View Events'
        )
        
        # Process status change events
        all_results['status_changes'] = self.process_file_in_chunks(
            files['status_change_events'], 
            self.process_status_change_events_chunk, 
            'Status Change Events'
        )
        
        logger.info("📈 Phase 2: Generating aggregated analytics...")
        self.generate_aggregated_analytics(all_results)
        
        logger.info("💾 Phase 3: Saving all output files...")
        self.save_all_outputs()
        
        elapsed_time = time.time() - start_time
        logger.info(f"🎉 ETL Pipeline completed successfully in {elapsed_time:.2f} seconds ({elapsed_time/60:.1f} minutes)")
        
        # Print summary statistics
        self.print_processing_summary(all_results, elapsed_time)

    def generate_aggregated_analytics(self, results: Dict[str, Any]):
        """Generate all aggregated analytics from processed results"""
        logger.info("   📊 Creating comprehensive analytics...")
        
        # Merge processed chunk results into main results
        for file_type, file_results in results.items():
            if file_type in self.processed_chunk_results:
                file_results.update(self.processed_chunk_results[file_type])
        
        # Use AnalyticsGenerator to create comprehensive analytics
        analytics_gen = AnalyticsGenerator()
        self.aggregated_data, self.csv_outputs = analytics_gen.process_results_to_analytics(results)
        
        logger.info("   ✅ Analytics generation completed!")

    def save_all_outputs(self):
        """Save all aggregated outputs to JSON and CSV files"""
        logger.info("   💾 Saving aggregated data files...")
        
        # Save main JSON file to both locations
        output_paths = [
            "/Users/iara/Projects/Zippotify_Datapipe/aggregated_music_data.json",
            "/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_music_data.json"
        ]
        
        for path in output_paths:
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(self.aggregated_data, f, indent=2, default=str)
                logger.info(f"   ✅ Saved JSON: {path}")
            except Exception as e:
                logger.error(f"   ❌ Error saving JSON to {path}: {e}")
        
        # Save CSV files to both root and dashboard directories
        csv_directories = [
            "/Users/iara/Projects/Zippotify_Datapipe",
            "/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data"
        ]
        
        for directory in csv_directories:
            try:
                os.makedirs(directory, exist_ok=True)
                for csv_name, df in self.csv_outputs.items():
                    csv_path = os.path.join(directory, f"{csv_name}.csv")
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    logger.info(f"   ✅ Saved CSV: {csv_path}")
            except Exception as e:
                logger.error(f"   ❌ Error saving CSV files to {directory}: {e}")
        
        logger.info(f"   📊 Total files saved: {len(output_paths)} JSON + {len(self.csv_outputs) * len(csv_directories)} CSV files")

    def print_processing_summary(self, results: Dict[str, Any], elapsed_time: float):
        """Print comprehensive processing summary"""
        logger.info("\n" + "="*80)
        logger.info("🎯 TRACKTIONAI ETL PIPELINE - PROCESSING SUMMARY")
        logger.info("="*80)
        logger.info(f"⏱️  Total Processing Time: {elapsed_time:.2f} seconds ({elapsed_time/60:.1f} minutes)")
        logger.info(f"📊 Total Records Processed: {sum(r.get('total_records', 0) for r in results.values()):,}")
        logger.info(f"🔄 Total Chunks Processed: {sum(r.get('processed_chunks', 0) for r in results.values())}")
        
        for file_type, data in results.items():
            logger.info(f"   📁 {file_type.replace('_', ' ').title()}: {data.get('total_records', 0):,} records")
        
        logger.info("="*80)


def main():
    """Main execution function"""
    try:
        # Initialize ETL pipeline with appropriate chunk size
        etl = TracktionAIETL(chunk_size=25000)  # Smaller chunks for memory safety
        
        # Run the complete pipeline
        etl.run_etl_pipeline()
        
        logger.info("🎉 All processing completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ ETL Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()
