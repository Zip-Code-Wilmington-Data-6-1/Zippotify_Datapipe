"""
Enhanced AI Genre Classifier with Multiple Cost-Effective AI Integration Options
Author: Sai
Date: September 21, 2025

This module demonstrates several minimal-cost AI approaches for music genre classification:
1. Local sentence transformers (FREE)
2. Hugging Face free inference API (FREE with rate limits)
3. OpenAI API with smart caching (LOW COST)
4. Hybrid rule-based + AI approach (COST EFFICIENT)
"""

import os
import json
import hashlib
import sqlite3
from typing import List, Dict, Optional, Tuple
import logging
from datetime import datetime
import requests
import numpy as np
from sentence_transformers import SentenceTransformer
from database import SessionLocal
from models import DimSong, DimGenre, DimSongGenre
import openai

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CostEffectiveAIClassifier:
    """
    A cost-effective AI-enhanced genre classifier using multiple strategies
    to minimize API costs while maximizing accuracy.
    """
    
    def __init__(self):
        self.db = SessionLocal()
        self.cache_db = self._setup_cache_db()
        
        # Load local sentence transformer model (FREE - runs locally)
        try:
            self.embedder = SentenceTransformer('all-MiniLM-L6-v2')  # Small, fast model
            logger.info("✅ Loaded local sentence transformer model")
        except Exception as e:
            logger.warning(f"⚠️ Could not load sentence transformer: {e}")
            self.embedder = None
        
        # Genre embeddings for similarity matching
        self.genre_embeddings = self._precompute_genre_embeddings()
        
        # AI API configurations
        self.openai_client = None
        self.huggingface_api_key = os.getenv('HUGGINGFACE_API_KEY')  # Free tier
        
        # Cost tracking
        self.api_calls_made = 0
        self.cache_hits = 0
        
    def _setup_cache_db(self) -> sqlite3.Connection:
        """Setup SQLite cache to avoid repeated AI API calls"""
        cache_path = 'ai_classification_cache.db'
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
        
    def _precompute_genre_embeddings(self) -> Dict[str, np.ndarray]:
        """Precompute embeddings for all genres for similarity matching"""
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
            'techno': 'techno electronic dance club beats music'
        }
        
        embeddings = {}
        for genre, description in genre_descriptions.items():
            try:
                embedding = self.embedder.encode(description)
                embeddings[genre] = embedding
                logger.info(f"📊 Generated embedding for {genre}")
            except Exception as e:
                logger.warning(f"⚠️ Could not generate embedding for {genre}: {e}")
                
        return embeddings
    
    def _get_song_hash(self, title: str, artist: str) -> str:
        """Generate unique hash for song to use as cache key"""
        song_string = f"{title.lower().strip()}_{artist.lower().strip()}"
        return hashlib.md5(song_string.encode()).hexdigest()
    
    def _check_cache(self, title: str, artist: str) -> Optional[Tuple[List[str], float, str]]:
        """Check if we've already classified this song"""
        song_hash = self._get_song_hash(title, artist)
        cursor = self.cache_db.execute(
            'SELECT predicted_genres, confidence_score, method_used FROM classification_cache WHERE song_hash = ?',
            (song_hash,)
        )
        result = cursor.fetchone()
        if result:
            self.cache_hits += 1
            genres = json.loads(result[0])
            return genres, result[1], result[2]
        return None
    
    def _save_to_cache(self, title: str, artist: str, genres: List[str], 
                       confidence: float, method: str):
        """Save classification result to cache"""
        song_hash = self._get_song_hash(title, artist)
        self.cache_db.execute('''
            INSERT OR REPLACE INTO classification_cache 
            (song_hash, song_title, artist_name, predicted_genres, confidence_score, method_used)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (song_hash, title, artist, json.dumps(genres), confidence, method))
        self.cache_db.commit()
    
    def classify_with_embeddings(self, title: str, artist: str) -> Tuple[List[str], float]:
        """
        FREE METHOD: Use local sentence transformers for similarity-based classification
        No API costs - runs entirely locally
        """
        if not self.embedder or not self.genre_embeddings:
            return [], 0.0
            
        # Create song description
        song_description = f"{title} by {artist} music song"
        
        try:
            # Generate song embedding
            song_embedding = self.embedder.encode(song_description)
            
            # Calculate similarities with all genres
            similarities = {}
            for genre, genre_embedding in self.genre_embeddings.items():
                similarity = np.dot(song_embedding, genre_embedding) / (
                    np.linalg.norm(song_embedding) * np.linalg.norm(genre_embedding)
                )
                similarities[genre] = similarity
            
            # Get top genres with similarity > threshold
            threshold = 0.3  # Adjust based on testing
            top_genres = [
                genre for genre, sim in similarities.items() 
                if sim > threshold
            ]
            
            if not top_genres:
                # If no genres meet threshold, take top 2
                top_genres = sorted(similarities.keys(), 
                                  key=lambda g: similarities[g], reverse=True)[:2]
            
            max_confidence = max(similarities.values())
            return top_genres[:3], max_confidence  # Limit to 3 genres
            
        except Exception as e:
            logger.error(f"Error in embedding classification: {e}")
            return [], 0.0
    
    def classify_with_huggingface_free(self, title: str, artist: str) -> Tuple[List[str], float]:
        """
        FREE METHOD: Use Hugging Face's free inference API
        Rate limited but completely free
        """
        if not self.huggingface_api_key:
            return [], 0.0
            
        # Use a pre-trained text classification model
        api_url = "https://api-inference.huggingface.co/models/facebook/bart-large-mnli"
        headers = {"Authorization": f"Bearer {self.huggingface_api_key}"}
        
        # Create candidate labels (genres)
        candidate_labels = ["pop", "rock", "hip-hop", "electronic", "jazz", "classical", 
                          "r&b", "country", "reggae", "metal", "punk", "folk", "latin"]
        
        payload = {
            "inputs": f"This song '{title}' by {artist} is a",
            "parameters": {"candidate_labels": candidate_labels}
        }
        
        try:
            response = requests.post(api_url, headers=headers, json=payload, timeout=10)
            if response.status_code == 200:
                result = response.json()
                self.api_calls_made += 1
                
                # Extract top predictions
                if 'labels' in result and 'scores' in result:
                    top_genres = []
                    for label, score in zip(result['labels'][:3], result['scores'][:3]):
                        if score > 0.3:  # Confidence threshold
                            top_genres.append(label)
                    
                    max_confidence = max(result['scores']) if result['scores'] else 0.0
                    return top_genres, max_confidence
                    
        except Exception as e:
            logger.warning(f"Hugging Face API error: {e}")
            
        return [], 0.0
    
    def classify_with_openai_smart(self, title: str, artist: str) -> Tuple[List[str], float]:
        """
        LOW COST METHOD: Use OpenAI API with smart prompting and caching
        Costs ~$0.0001-0.001 per classification
        """
        if not self.openai_client:
            try:
                openai.api_key = os.getenv('OPENAI_API_KEY')
                self.openai_client = openai
            except:
                return [], 0.0
        
        # Smart, concise prompt to minimize token usage
        prompt = f"""Genre classify: "{title}" by {artist}
        
Return only JSON: {{"genres": ["genre1", "genre2"], "confidence": 0.85}}

Available genres: pop, rock, hip-hop, electronic, jazz, classical, r&b, country, reggae, metal, punk, folk, latin, blues, indie, gospel, funk, soul, ambient, techno"""
        
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",  # Cheapest model
                messages=[{"role": "user", "content": prompt}],
                max_tokens=50,  # Minimize cost
                temperature=0.3
            )
            
            self.api_calls_made += 1
            result_text = response.choices[0].message.content.strip()
            
            # Parse JSON response
            import json
            result = json.loads(result_text)
            return result.get('genres', []), result.get('confidence', 0.0)
            
        except Exception as e:
            logger.warning(f"OpenAI API error: {e}")
            return [], 0.0
    
    def hybrid_classify(self, title: str, artist: str) -> List[str]:
        """
        HYBRID METHOD: Combine rule-based + AI for best cost/accuracy balance
        Uses rule-based as primary, AI for edge cases only
        """
        from ai_genre_classifier import rule_based_genre_classification
        
        # Check cache first
        cached_result = self._check_cache(title, artist)
        if cached_result:
            return cached_result[0]
        
        # Try rule-based first (FREE)
        rule_genres = rule_based_genre_classification(title, artist)
        
        # If rule-based gives good results, use them
        if len(rule_genres) >= 1 and 'pop' not in rule_genres:
            self._save_to_cache(title, artist, rule_genres, 0.8, 'rule-based')
            return rule_genres
        
        # For ambiguous cases, use AI
        ai_genres, confidence = [], 0.0
        
        # Try local embeddings first (FREE)
        if self.embedder:
            ai_genres, confidence = self.classify_with_embeddings(title, artist)
            if confidence > 0.4:
                self._save_to_cache(title, artist, ai_genres, confidence, 'embeddings')
                return ai_genres
        
        # For critical cases, use API (SMALL COST)
        if confidence < 0.4:
            # Try free Hugging Face first
            ai_genres, confidence = self.classify_with_huggingface_free(title, artist)
            if confidence > 0.5:
                self._save_to_cache(title, artist, ai_genres, confidence, 'huggingface')
                return ai_genres
            
            # As last resort, use OpenAI (MINIMAL COST)
            if confidence < 0.5:
                ai_genres, confidence = self.classify_with_openai_smart(title, artist)
                if ai_genres:
                    self._save_to_cache(title, artist, ai_genres, confidence, 'openai')
                    return ai_genres
        
        # Fallback to rule-based result
        self._save_to_cache(title, artist, rule_genres, 0.6, 'rule-fallback')
        return rule_genres
    
    def get_cost_stats(self) -> Dict:
        """Get cost and performance statistics"""
        return {
            'api_calls_made': self.api_calls_made,
            'cache_hits': self.cache_hits,
            'estimated_cost_usd': self.api_calls_made * 0.0005,  # Conservative estimate
            'cache_hit_rate': self.cache_hits / max(1, self.api_calls_made + self.cache_hits)
        }
    
    def process_songs_with_ai(self, batch_size: int = 100, start_id: int = 1):
        """Process songs using hybrid AI approach"""
        songs = self.db.query(DimSong).filter(DimSong.song_id >= start_id).limit(batch_size).all()
        
        for song in songs:
            # Clear existing genre assignments
            self.db.query(DimSongGenre).filter(DimSongGenre.song_id == song.song_id).delete()
            
            # Get AI-enhanced classification
            predicted_genres = self.hybrid_classify(song.title, song.artist_name or "Unknown")
            
            # Assign genres
            for genre_name in predicted_genres:
                genre = self.db.query(DimGenre).filter(DimGenre.genre_name == genre_name).first()
                if genre:
                    song_genre = DimSongGenre(song_id=song.song_id, genre_id=genre.genre_id)
                    self.db.add(song_genre)
            
            print(f"🤖 AI Enhanced: '{song.title}' by {song.artist_name} -> {predicted_genres}")
        
        self.db.commit()
        
        # Print cost statistics
        stats = self.get_cost_stats()
        print(f"\n💰 Cost Statistics:")
        print(f"   API calls made: {stats['api_calls_made']}")
        print(f"   Cache hits: {stats['cache_hits']}")
        print(f"   Estimated cost: ${stats['estimated_cost_usd']:.4f}")
        print(f"   Cache hit rate: {stats['cache_hit_rate']:.1%}")

# Usage example
if __name__ == "__main__":
    classifier = CostEffectiveAIClassifier()
    
    # Test the different methods
    test_songs = [
        ("Bohemian Rhapsody", "Queen"),
        ("Lose Yourself", "Eminem"),
        ("Hotel California", "Eagles")
    ]
    
    for title, artist in test_songs:
        print(f"\n🎵 Testing: '{title}' by {artist}")
        
        # Try embedding method
        genres, conf = classifier.classify_with_embeddings(title, artist)
        print(f"  📊 Embeddings: {genres} (conf: {conf:.3f})")
        
        # Try hybrid method
        hybrid_genres = classifier.hybrid_classify(title, artist)
        print(f"  🤖 Hybrid: {hybrid_genres}")