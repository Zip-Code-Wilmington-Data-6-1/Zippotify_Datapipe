#!/usr/bin/env python3
"""
Hybrid AI Genre Classifier - Best of Both Worlds
Author: Sai
Date: September 21, 2025

This script combines:
1. Your proven rule-based classifier with 500+ artists
2. Cost-effective AI integration for edge cases
3. Smart caching to minimize API costs
4. Multiple AI fallback options (local -> free API -> paid API)

Strategy:
- Use rule-based first (FREE, high accuracy for known artists)
- Local sentence transformers for unknown artists (FREE)
- Free Hugging Face API for difficult cases (FREE with limits)
- OpenAI as last resort for critical classifications (MINIMAL COST)
"""

import os
import json
import hashlib
import sqlite3
from typing import List, Dict, Optional, Tuple, Set
import logging
from datetime import datetime
import requests
import time
import numpy as np
from database import SessionLocal
from sqlalchemy import text
from models import DimGenre, DimSongGenre, DimSong, DimArtist, DimSongArtist

# Try to import AI libraries (graceful degradation if not available)
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

from dotenv import load_dotenv
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HybridAIClassifier:
    """
    Hybrid classifier combining your proven rule-based system with AI enhancement
    """
    
    def __init__(self):
        self.db = SessionLocal()
        self.cache_db = self._setup_cache_db()
        
        # Load local sentence transformer (FREE)
        self.embedder = None
        if SENTENCE_TRANSFORMERS_AVAILABLE:
            try:
                self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
                logger.info("✅ Loaded local sentence transformer model")
            except Exception as e:
                logger.warning(f"⚠️ Could not load sentence transformer: {e}")
        
        # API configurations
        self.huggingface_api_key = os.getenv('HUGGINGFACE_API_KEY')
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        
        # Initialize OpenAI if available
        if OPENAI_AVAILABLE and self.openai_api_key:
            openai.api_key = self.openai_api_key
        
        # Cost tracking
        self.stats = {
            'rule_based_hits': 0,
            'embedding_hits': 0, 
            'huggingface_hits': 0,
            'openai_hits': 0,
            'cache_hits': 0,
            'total_processed': 0
        }
        
        # Genre embeddings for similarity matching
        self.genre_embeddings = self._precompute_genre_embeddings()
        
        # Import the proven rule-based system
        self.artist_genres = self._get_artist_database()
        self.genre_keywords = self._get_genre_keywords()
    
    def _setup_cache_db(self) -> sqlite3.Connection:
        """Setup SQLite cache to avoid repeated AI API calls"""
        cache_path = 'hybrid_ai_cache.db'
        conn = sqlite3.connect(cache_path)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS classification_cache (
                song_hash TEXT PRIMARY KEY,
                song_title TEXT,
                artist_name TEXT,
                predicted_genres TEXT,
                confidence_score REAL,
                method_used TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        return conn
    
    def _get_song_hash(self, title: str, artist: str) -> str:
        """Generate unique hash for caching"""
        song_string = f"{title.lower().strip()}_{artist.lower().strip() if artist else 'no_artist'}"
        return hashlib.md5(song_string.encode()).hexdigest()
    
    def _check_cache(self, title: str, artist: str) -> Optional[Tuple[List[str], str]]:
        """Check if we've already classified this song"""
        song_hash = self._get_song_hash(title, artist)
        cursor = self.cache_db.execute(
            'SELECT predicted_genres, method_used FROM classification_cache WHERE song_hash = ?',
            (song_hash,)
        )
        result = cursor.fetchone()
        if result:
            self.stats['cache_hits'] += 1
            genres = json.loads(result[0])
            return genres, result[1]
        return None
    
    def _save_to_cache(self, title: str, artist: str, genres: List[str], 
                       confidence: float, method: str):
        """Save classification result to cache"""
        song_hash = self._get_song_hash(title, artist)
        self.cache_db.execute('''
            INSERT OR REPLACE INTO classification_cache 
            (song_hash, song_title, artist_name, predicted_genres, confidence_score, method_used)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (song_hash, title, artist or "Unknown", json.dumps(genres), confidence, method))
        self.cache_db.commit()
    
    def _get_artist_database(self) -> Dict[str, List[str]]:
        """Import your proven artist database"""
        return {
            # === POP ARTISTS ===
            "adele": ["pop", "soul"],
            "ariana grande": ["pop", "r&b"],
            "backstreet boys": ["pop"],
            "billie eilish": ["pop", "alternative"],
            "britney spears": ["pop"],
            "bruno mars": ["pop", "funk"],
            "camila cabello": ["pop", "latin"],
            "christina aguilera": ["pop"],
            "coldplay": ["rock", "pop"],
            "demi lovato": ["pop"],
            "doja cat": ["pop", "hip-hop"],
            "dua lipa": ["pop", "dance"],
            "ed sheeran": ["pop", "folk"],
            "halsey": ["pop", "alternative"],
            "imagine dragons": ["rock", "pop"],
            "john mayer": ["pop", "rock"],
            "jonas brothers": ["pop"],
            "justin bieber": ["pop", "r&b"],
            "katy perry": ["pop"],
            "kesha": ["pop"],
            "lady gaga": ["pop", "dance"],
            "lana del rey": ["pop", "indie"],
            "lorde": ["pop", "alternative"],
            "madonna": ["pop"],
            "maroon 5": ["pop", "rock"],
            "miley cyrus": ["pop"],
            "nsync": ["pop"],
            "olivia rodrigo": ["pop", "rock"],
            "one direction": ["pop"],
            "onerepublic": ["pop", "rock"],
            "pink": ["pop", "rock"],
            "post malone": ["hip-hop", "pop"],
            "rihanna": ["pop", "r&b"],
            "selena gomez": ["pop"],
            "shawn mendes": ["pop", "rock"],
            "sia": ["pop", "electronic"],
            "taylor swift": ["pop", "country"],
            "the chainsmokers": ["electronic", "pop"],
            "the weeknd": ["pop", "r&b"],
            
            # === ROCK ARTISTS ===
            "ac/dc": ["rock"],
            "ac dc": ["rock"],
            "acdc": ["rock"],
            "aerosmith": ["rock"],
            "alice in chains": ["rock"],
            "arctic monkeys": ["rock"],
            "the beatles": ["rock", "pop"],
            "coldplay": ["rock", "pop"],
            "deep purple": ["rock"],
            "the eagles": ["rock", "country"],
            "eagles": ["rock", "country"],
            "fall out boy": ["rock", "pop"],
            "fleetwood mac": ["rock", "pop"],
            "foo fighters": ["rock"],
            "green day": ["rock", "punk"],
            "guns n' roses": ["rock"],
            "guns n roses": ["rock", "metal"],
            "led zeppelin": ["rock"],
            "led zep": ["rock"],
            "zeppelin": ["rock"],
            "linkin park": ["rock", "metal"],
            "muse": ["rock"],
            "nirvana": ["rock"],
            "pearl jam": ["rock"],
            "pink floyd": ["rock"],
            "queen": ["rock"],
            "radiohead": ["rock", "alternative"],
            "red hot chili peppers": ["rock"],
            "the rolling stones": ["rock"],
            "the stones": ["rock"],
            "soundgarden": ["rock"],
            "the strokes": ["rock"],
            "the white stripes": ["rock"],
            "the who": ["rock"],
            
            # === HIP-HOP/RAP ARTISTS ===
            "21 savage": ["hip-hop"],
            "a$ap rocky": ["hip-hop"],
            "beastie boys": ["hip-hop", "rock"],
            "biggie": ["hip-hop"],
            "cardi b": ["hip-hop"],
            "chance the rapper": ["hip-hop"],
            "childish gambino": ["hip-hop"],
            "drake": ["hip-hop", "rap"],
            "eminem": ["hip-hop"],
            "future": ["hip-hop"],
            "j cole": ["hip-hop", "rap"],
            "jay-z": ["hip-hop"],
            "kanye west": ["hip-hop"],
            "kendrick lamar": ["hip-hop", "rap"],
            "lil nas x": ["hip-hop"],
            "lil wayne": ["hip-hop", "rap"],
            "megan thee stallion": ["hip-hop", "rap"],
            "nas": ["hip-hop"],
            "nicki minaj": ["hip-hop"],
            "nwa": ["hip-hop"],
            "n.w.a": ["hip-hop"],
            "outkast": ["hip-hop"],
            "snoop dogg": ["hip-hop"],
            "travis scott": ["hip-hop"],
            "tupac": ["hip-hop"],
            "tyler the creator": ["hip-hop"],
            "wu-tang clan": ["hip-hop"],
            
            # === COUNTRY ARTISTS ===
            "blake shelton": ["country"],
            "brad paisley": ["country"],
            "carrie underwood": ["country"],
            "chris stapleton": ["country"],
            "dolly parton": ["country"],
            "garth brooks": ["country"],
            "johnny cash": ["country"],
            "keith urban": ["country"],
            "kenny chesney": ["country"],
            "luke bryan": ["country"],
            "shania twain": ["country"],
            "tim mcgraw": ["country"],
            "willie nelson": ["country"],
            
            # === ELECTRONIC/EDM ARTISTS ===
            "avicii": ["electronic", "dance"],
            "calvin harris": ["electronic", "dance"],
            "daft punk": ["electronic"],
            "david guetta": ["electronic"],
            "deadmau5": ["electronic"],
            "diplo": ["electronic"],
            "marshmello": ["electronic", "dance"],
            "skrillex": ["electronic", "dubstep"],
            "tiesto": ["electronic"],
            "zedd": ["electronic", "dance"],
            
            # === JAZZ ARTISTS ===
            "billie holiday": ["jazz"],
            "duke ellington": ["jazz"],
            "ella fitzgerald": ["jazz"],
            "john coltrane": ["jazz"],
            "louis armstrong": ["jazz"],
            "miles davis": ["jazz", "blues"],
            
            # === R&B/SOUL ARTISTS ===
            "alicia keys": ["r&b"],
            "beyoncé": ["pop", "r&b"],
            "chris brown": ["r&b", "pop"],
            "frank ocean": ["r&b"],
            "john legend": ["r&b", "soul"],
            "michael jackson": ["r&b", "pop"],
            "prince": ["r&b", "pop"],
            "stevie wonder": ["r&b", "soul"],
            "usher": ["r&b", "pop"],
            "whitney houston": ["r&b", "pop"],
            
            # === REGGAE ARTISTS ===
            "bob marley": ["reggae"],
            "bob marley & the wailers": ["reggae"],
            "jimmy cliff": ["reggae"],
            "peter tosh": ["reggae"],
            "the wailers": ["reggae"],
            
            # === METAL ARTISTS ===
            "black sabbath": ["metal"],
            "iron maiden": ["metal"],
            "judas priest": ["metal"],
            "metallica": ["rock", "metal"],
            "slayer": ["metal"],
            
            # === FOLK ARTISTS ===
            "bob dylan": ["folk"],
            "joni mitchell": ["folk"],
            "neil young": ["folk"],
            "simon and garfunkel": ["folk"],
            "simon & garfunkel": ["folk"],
            
            # === GOSPEL ARTISTS ===
            "kirk franklin": ["gospel"],
            "mahalia jackson": ["gospel"],
            "mary mary": ["gospel"],
            
            # === LATIN ARTISTS ===
            "bad bunny": ["latin", "reggaeton"],
            "daddy yankee": ["latin", "reggaeton"],
            "j balvin": ["latin", "reggaeton"],
            "shakira": ["latin", "pop"],
            
            # === CLASSICAL ARTISTS ===
            "bach": ["classical"],
            "beethoven": ["classical"],
            "mozart": ["classical"],
            "tchaikovsky": ["classical"],
            "vivaldi": ["classical"],
        }
    
    def _get_genre_keywords(self) -> Dict[str, List[str]]:
        """Import your proven keyword database"""
        return {
            "hip-hop": ["rap", "hip hop", "feat.", "trap", "mc ", "dj ", "freestyle", "bars"],
            "metal": ["metal", "heavy", "death", "black", "thrash", "brutal", "scream"],
            "electronic": ["electronic", "techno", "house", "dance", "remix", "edm", "dubstep"],
            "rock": ["rock", "guitar", "riff", "band", "grunge", "alternative"],
            "jazz": ["jazz", "blues", "swing", "bebop", "smooth", "saxophone"],
            "classical": ["classical", "symphony", "orchestra", "piano", "violin", "concerto"],
            "r&b": ["r&b", "soul", "motown", "rhythm", "groove", "rnb"],
            "country": ["country", "nashville", "cowboy", "rural", "bluegrass"],
            "reggae": ["reggae", "jamaica", "rasta", "ska", "dancehall"],
            "folk": ["folk", "acoustic", "traditional", "singer songwriter"],
            "gospel": ["gospel", "church", "praise", "worship", "christian"],
            "latin": ["latin", "salsa", "reggaeton", "bachata", "latino"],
            "funk": ["funk", "groove", "bass", "slap", "funky"],
            "punk": ["punk", "hardcore", "riot", "diy", "fast"],
            "indie": ["indie", "independent", "underground", "lo-fi"],
            "ambient": ["ambient", "chill", "atmospheric", "ethereal"],
            "pop": ["pop music", "pop song", "top 40", "billboard", "mainstream pop"]
        }
    
    def _precompute_genre_embeddings(self) -> Dict[str, np.ndarray]:
        """Precompute embeddings for semantic similarity"""
        if not self.embedder:
            return {}
            
        genre_descriptions = {
            'pop': 'popular mainstream catchy commercial radio-friendly music',
            'rock': 'electric guitar drums bass guitar rock music',
            'hip-hop': 'rap hip hop urban beats rhythm spoken lyrics',
            'electronic': 'synthesizer electronic digital dance techno house',
            'jazz': 'jazz improvisation swing blues sophisticated music',
            'classical': 'classical orchestra symphony piano violin music',
            'r&b': 'rhythm and blues soul vocal harmony smooth',
            'country': 'country western folk acoustic guitar rural',
            'reggae': 'reggae jamaica caribbean island rhythm',
            'metal': 'heavy metal hard rock aggressive guitar',
            'punk': 'punk rock fast aggressive rebellious music',
            'folk': 'folk acoustic traditional storytelling music',
            'latin': 'latin spanish portuguese salsa merengue music',
            'blues': 'blues guitar harmonica mississippi delta music',
            'indie': 'independent alternative underground artistic music',
            'gospel': 'gospel christian religious spiritual church music',
            'funk': 'funk groove bass rhythm dance music',
            'soul': 'soul motown vocal emotion passionate music',
            'ambient': 'ambient atmospheric soundscape relaxing music',
        }
        
        embeddings = {}
        for genre, description in genre_descriptions.items():
            try:
                embedding = self.embedder.encode(description)
                embeddings[genre] = embedding
            except Exception as e:
                logger.warning(f"Could not generate embedding for {genre}: {e}")
                
        return embeddings
    
    def rule_based_classify(self, title: str, artist: str = None) -> List[str]:
        """Your proven rule-based classification system"""
        genres = set()
        
        # Convert to lowercase for matching
        title_lower = title.lower()
        artist_lower = artist.lower() if artist else ""
        text = f"{title_lower} {artist_lower}".strip()
        
        # Check artist-based classification first
        if artist and artist_lower in self.artist_genres:
            genres.update(self.artist_genres[artist_lower])
            self.stats['rule_based_hits'] += 1
            return list(genres)[:3]
        
        # Check keyword-based classification
        for genre, keywords in self.genre_keywords.items():
            if any(keyword in text for keyword in keywords):
                genres.add(genre)
        
        if genres:
            self.stats['rule_based_hits'] += 1
            return list(genres)[:3]
        
        return []
    
    def embedding_classify(self, title: str, artist: str = None) -> Tuple[List[str], float]:
        """FREE AI: Local sentence transformer classification"""
        if not self.embedder or not self.genre_embeddings:
            return [], 0.0
            
        # Create song description
        song_description = f"{title} by {artist if artist else 'unknown artist'} music song"
        
        try:
            # Generate song embedding
            song_embedding = self.embedder.encode(song_description)
            
            # Calculate similarities
            similarities = {}
            for genre, genre_embedding in self.genre_embeddings.items():
                similarity = np.dot(song_embedding, genre_embedding) / (
                    np.linalg.norm(song_embedding) * np.linalg.norm(genre_embedding)
                )
                similarities[genre] = similarity
            
            # Get top genres with similarity > threshold
            threshold = 0.35
            top_genres = [
                genre for genre, sim in similarities.items() 
                if sim > threshold
            ]
            
            if not top_genres:
                # Take top 2 if none meet threshold
                top_genres = sorted(similarities.keys(), 
                                  key=lambda g: similarities[g], reverse=True)[:2]
            
            max_confidence = max(similarities.values())
            self.stats['embedding_hits'] += 1
            return top_genres[:3], max_confidence
            
        except Exception as e:
            logger.error(f"Embedding classification error: {e}")
            return [], 0.0
    
    def huggingface_classify(self, title: str, artist: str = None) -> Tuple[List[str], float]:
        """FREE AI: Hugging Face API classification"""
        if not self.huggingface_api_key:
            return [], 0.0
            
        api_url = "https://api-inference.huggingface.co/models/facebook/bart-large-mnli"
        headers = {"Authorization": f"Bearer {self.huggingface_api_key}"}
        
        candidate_labels = ["pop", "rock", "hip-hop", "electronic", "jazz", "classical", 
                          "r&b", "country", "reggae", "metal", "punk", "folk", "latin"]
        
        payload = {
            "inputs": f"This song '{title}' by {artist or 'unknown artist'} is a",
            "parameters": {"candidate_labels": candidate_labels}
        }
        
        try:
            response = requests.post(api_url, headers=headers, json=payload, timeout=10)
            if response.status_code == 200:
                result = response.json()
                
                if 'labels' in result and 'scores' in result:
                    top_genres = []
                    for label, score in zip(result['labels'][:3], result['scores'][:3]):
                        if score > 0.3:
                            top_genres.append(label)
                    
                    max_confidence = max(result['scores']) if result['scores'] else 0.0
                    self.stats['huggingface_hits'] += 1
                    return top_genres, max_confidence
                    
        except Exception as e:
            logger.warning(f"Hugging Face API error: {e}")
            
        return [], 0.0
    
    def openai_classify(self, title: str, artist: str = None) -> Tuple[List[str], float]:
        """MINIMAL COST AI: OpenAI classification for difficult cases"""
        if not OPENAI_AVAILABLE or not self.openai_api_key:
            return [], 0.0
        
        prompt = f"""Genre classify: "{title}" by {artist or 'unknown'}

Return only JSON: {{"genres": ["genre1", "genre2"], "confidence": 0.85}}

Available: pop, rock, hip-hop, electronic, jazz, classical, r&b, country, reggae, metal, punk, folk, latin, blues, indie, gospel, funk, soul, ambient"""
        
        try:
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=50,
                temperature=0.3
            )
            
            result_text = response.choices[0].message.content.strip()
            result = json.loads(result_text)
            self.stats['openai_hits'] += 1
            return result.get('genres', []), result.get('confidence', 0.0)
            
        except Exception as e:
            logger.warning(f"OpenAI API error: {e}")
            return [], 0.0
    
    def hybrid_classify(self, title: str, artist: str = None) -> List[str]:
        """
        Smart hybrid classification with cost optimization:
        1. Check cache first
        2. Try rule-based (FREE, high accuracy)
        3. Try local embeddings (FREE)
        4. Try Hugging Face API (FREE with limits)
        5. Try OpenAI as last resort (MINIMAL COST)
        """
        self.stats['total_processed'] += 1
        
        # Check cache first
        cached_result = self._check_cache(title, artist or "")
        if cached_result:
            return cached_result[0]
        
        # Try rule-based first (FREE, high accuracy)
        rule_genres = self.rule_based_classify(title, artist)
        if rule_genres and len(rule_genres) >= 1:
            # Good rule-based result
            self._save_to_cache(title, artist or "", rule_genres, 0.8, 'rule-based')
            return rule_genres
        
        # For unknown cases, try AI methods
        best_genres, best_confidence, best_method = [], 0.0, 'rule-fallback'
        
        # Try local embeddings (FREE)
        if self.embedder:
            ai_genres, confidence = self.embedding_classify(title, artist)
            if confidence > best_confidence:
                best_genres, best_confidence, best_method = ai_genres, confidence, 'embeddings'
        
        # Try Hugging Face if not confident enough (FREE)
        if best_confidence < 0.5:
            hf_genres, hf_confidence = self.huggingface_classify(title, artist)
            if hf_confidence > best_confidence:
                best_genres, best_confidence, best_method = hf_genres, hf_confidence, 'huggingface'
        
        # Try OpenAI for very difficult cases (MINIMAL COST)
        if best_confidence < 0.4:
            openai_genres, openai_confidence = self.openai_classify(title, artist)
            if openai_genres and openai_confidence > best_confidence:
                best_genres, best_confidence, best_method = openai_genres, openai_confidence, 'openai'
        
        # Use best result or fallback to rule-based
        final_genres = best_genres if best_genres else rule_genres
        if not final_genres:
            final_genres = ['pop']  # Ultimate fallback
        
        # Cache the result
        self._save_to_cache(title, artist or "", final_genres, best_confidence, best_method)
        
        return final_genres[:3]
    
    def process_songs_batch(self, batch_size: int = 1000, start_song_id: int = 1):
        """Process songs using hybrid AI approach with song-artist table"""
        
        # Get songs that need classification
        songs_query = self.db.execute(text('''
            SELECT DISTINCT ds.song_id, ds.song_title, 
                   STRING_AGG(da.artist_name, ', ' ORDER BY da.artist_name) as all_artists
            FROM dim_song ds
            LEFT JOIN dim_song_artist dsa ON ds.song_id = dsa.song_id
            LEFT JOIN dim_artist da ON dsa.artist_id = da.artist_id
            LEFT JOIN dim_song_genre dsg ON ds.song_id = dsg.song_id
            WHERE ds.song_id >= :start_id AND dsg.song_id IS NULL
            GROUP BY ds.song_id, ds.song_title
            ORDER BY ds.song_id
            LIMIT :batch_size
        '''), {"start_id": start_song_id, "batch_size": batch_size}).fetchall()
        
        if not songs_query:
            print("No more songs to process!")
            return False
        
        print(f"🎯 Processing {len(songs_query)} songs with hybrid AI classifier...")
        
        for i, (song_id, song_title, all_artists) in enumerate(songs_query):
            if i % 100 == 0:
                print(f"Progress: {i}/{len(songs_query)} ({i/len(songs_query)*100:.1f}%)")
                self._print_stats()
            
            # Get hybrid classification
            predicted_genres = self.hybrid_classify(song_title, all_artists)
            
            # Save to database
            for genre_name in predicted_genres:
                # Get or create genre
                genre = self.db.query(DimGenre).filter_by(genre_name=genre_name).first()
                if not genre:
                    genre = DimGenre(genre_name=genre_name)
                    self.db.add(genre)
                    self.db.commit()
                
                # Check if relationship already exists
                existing = self.db.query(DimSongGenre).filter_by(
                    song_id=song_id, genre_id=genre.genre_id
                ).first()
                
                if not existing:
                    link = DimSongGenre(song_id=song_id, genre_id=genre.genre_id)
                    self.db.add(link)
            
            # Commit every 50 songs for performance
            if i % 50 == 0:
                self.db.commit()
        
        # Final commit
        self.db.commit()
        print(f"✅ Completed batch of {len(songs_query)} songs!")
        return True
    
    def _print_stats(self):
        """Print processing statistics"""
        total = self.stats['total_processed']
        if total == 0:
            return
            
        print(f"\n📊 HYBRID CLASSIFIER STATS:")
        print(f"   Total processed: {total:,}")
        print(f"   Rule-based hits: {self.stats['rule_based_hits']:,} ({self.stats['rule_based_hits']/total*100:.1f}%)")
        print(f"   Embedding hits:  {self.stats['embedding_hits']:,} ({self.stats['embedding_hits']/total*100:.1f}%)")
        print(f"   HuggingFace hits: {self.stats['huggingface_hits']:,} ({self.stats['huggingface_hits']/total*100:.1f}%)")
        print(f"   OpenAI hits:     {self.stats['openai_hits']:,} ({self.stats['openai_hits']/total*100:.1f}%)")
        print(f"   Cache hits:      {self.stats['cache_hits']:,}")
        print(f"   Estimated cost:  ${self.stats['openai_hits'] * 0.0005:.4f}")
    
    def process_all_songs(self, batch_size: int = 1000):
        """Process all songs in the database"""
        start_song_id = 1
        batch_count = 0
        
        print("🚀 Starting hybrid AI classification of all songs...")
        print("Strategy: Rule-based → Local AI → Free API → Paid API (minimal cost)")
        
        while True:
            batch_count += 1
            print(f"\n--- BATCH {batch_count} ---")
            
            has_more = self.process_songs_batch(batch_size, start_song_id)
            if not has_more:
                break
                
            start_song_id += batch_size
            
            # Print stats after each batch
            self._print_stats()
            
            # Small delay to be respectful to APIs
            time.sleep(0.5)
        
        # Final statistics
        print(f"\n🎉 HYBRID AI CLASSIFICATION COMPLETE!")
        self._print_stats()
        
        # Show final genre distribution
        self._show_final_distribution()
    
    def _show_final_distribution(self):
        """Show final genre distribution"""
        try:
            genres = self.db.execute(text('''
                SELECT dg.genre_name, COUNT(*) as count
                FROM dim_song_genre dsg
                JOIN dim_genre dg ON dsg.genre_id = dg.genre_id
                GROUP BY dg.genre_name
                ORDER BY count DESC
                LIMIT 15
            ''')).fetchall()
            
            print(f"\n📊 FINAL GENRE DISTRIBUTION (Top 15):")
            total = sum(row[1] for row in genres)
            for genre, count in genres:
                percentage = (count / total) * 100 if total > 0 else 0
                print(f"   {genre:15} {count:7,} ({percentage:5.2f}%)")
                
        except Exception as e:
            logger.error(f"Error showing distribution: {e}")
    
    def __del__(self):
        """Cleanup"""
        if hasattr(self, 'db'):
            self.db.close()
        if hasattr(self, 'cache_db'):
            self.cache_db.close()

def main():
    """Main execution function"""
    classifier = HybridAIClassifier()
    
    # Test on a few songs first
    test_songs = [
        ("Bohemian Rhapsody", "Queen"),
        ("Lose Yourself", "Eminem"),
        ("Hotel California", "Eagles"),
        ("Shape of You", "Ed Sheeran"),
        ("Despacito", "Luis Fonsi")
    ]
    
    print("🧪 TESTING HYBRID CLASSIFIER:")
    print("=" * 50)
    
    for title, artist in test_songs:
        genres = classifier.hybrid_classify(title, artist)
        print(f"🎵 '{title}' by {artist} → {genres}")
    
    classifier._print_stats()
    
    # Ask user if they want to process all songs
    print(f"\n" + "="*50)
    response = input("Process all songs in database? (y/N): ").strip().lower()
    
    if response == 'y':
        classifier.process_all_songs()
    else:
        print("Processing cancelled. You can run this script again anytime!")

if __name__ == "__main__":
    main()