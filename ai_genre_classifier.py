# import openai  # Uncomment when you want to use OpenAI
import time
import json
from database import SessionLocal
from sqlalchemy import text
from models import DimGenre, DimSongGenre
import os
from dotenv import load_dotenv

load_dotenv()

# You'll need to set OPENAI_API_KEY in your .env file and install openai
# openai.api_key = os.getenv("OPENAI_API_KEY")

def classify_genres_with_llm(song_title, artist_name):
    """Use LLM to classify song genres based on title and artist"""
    
    prompt = f"""
    Given the song title "{song_title}" by artist "{artist_name}", predict the most likely music genres.
    
    Return ONLY a JSON list of 1-3 most likely genres from this list:
    ["rock", "pop", "hip-hop", "rap", "country", "jazz", "blues", "electronic", "dance", 
     "classical", "folk", "metal", "punk", "reggae", "r&b", "soul", "funk", "indie", 
     "alternative", "acoustic", "soundtrack", "ambient", "house", "techno", "dubstep",
     "gospel", "latin", "world", "experimental", "instrumental"]
    
    Example response: ["rock", "alternative"]
    
    Response:
    """
    
    try:
        # Using OpenAI GPT (you'd need to uncomment and configure)
        # response = openai.ChatCompletion.create(
        #     model="gpt-3.5-turbo",
        #     messages=[{"role": "user", "content": prompt}],
        #     max_tokens=100,
        #     temperature=0.3
        # )
        # genres = json.loads(response.choices[0].message.content)
        
        # For demo - rule-based classification
        genres = rule_based_genre_classification(song_title, artist_name)
        return genres
        
    except Exception as e:
        print(f"Error classifying {song_title}: {e}")
        return []

def rule_based_genre_classification(song_title, artist_name=None):
    """Rule-based genre classification with improved diversity"""
    genres = []
    
    # Convert to lowercase for matching
    title_lower = song_title.lower()
    artist_lower = artist_name.lower() if artist_name else ""
    text = f"{title_lower} {artist_lower}".strip()
    
    # Enhanced genre keywords and patterns for better diversity
    genre_keywords = {
        "hip-hop": ["rap", "hip hop", "feat.", "nigga", "gangsta", "trap", "mc", "dj", "crew", "posse", "freestyle", "cipher"],
        "rock": ["rock", "metal", "punk", "guitar", "riff", "shred", "headbang", "grunge", "alternative", "garage"],
        "electronic": ["electronic", "techno", "house", "dance", "remix", "mix", "beat", "synth", "edm", "trance", "dubstep"],
        "country": ["country", "nashville", "cowboy", "rural", "honky", "bluegrass", "fiddle", "banjo", "americana"],
        "jazz": ["jazz", "blues", "swing", "bebop", "smooth", "fusion", "improvisation", "saxophone", "trumpet"],
        "classical": ["classical", "symphony", "orchestra", "piano", "violin", "concerto", "sonata", "chamber", "baroque"],
        "pop": ["pop", "radio", "hit", "chart", "mainstream", "catchy", "single", "top 40"],
        "r&b": ["r&b", "soul", "motown", "funk", "rhythm", "groove", "smooth", "neo soul"],
        "reggae": ["reggae", "jamaica", "rasta", "ska", "dancehall", "caribbean", "island", "dub"],
        "folk": ["folk", "acoustic", "traditional", "singer songwriter", "storytelling", "campfire", "unplugged"],
        "gospel": ["gospel", "church", "praise", "worship", "jesus", "god", "spiritual", "choir", "hymn", "christian"],
        "latin": ["latin", "spanish", "salsa", "mariachi", "cumbia", "reggaeton", "bachata", "merengue", "tropical"],
        "soundtrack": ["soundtrack", "theme", "score", "movie", "film", "cinema", "television", "tv", "game"],
        "ambient": ["ambient", "chill", "meditation", "atmospheric", "ethereal", "drone", "soundscape"],
        "indie": ["indie", "independent", "underground", "lo-fi", "bedroom", "artsy", "experimental"],
        "funk": ["funk", "groove", "bass", "slap", "tight", "pocket", "rhythm section"],
        "metal": ["metal", "heavy", "death", "black", "thrash", "doom", "brutal", "headbang"],
        "punk": ["punk", "hardcore", "riot", "anarchy", "diy", "fast", "aggressive", "rebellious"]
    }
    
    # Known artists by genre (you could expand this)
    artist_genres = {
        "john mayer": ["pop", "rock"],
        "ludovico einaudi": ["classical", "instrumental"],
        "paul baloche": ["gospel"],
        "afrika bambaataa": ["hip-hop", "electronic"],
        "skream": ["electronic", "dubstep"],
        "we the kings": ["pop", "rock"],
        "denison witmer": ["indie", "folk"],
        "cornel campbell": ["reggae"],
        "prophet posse": ["hip-hop", "rap"],
        "the beatles": ["rock", "pop"],
        "miles davis": ["jazz", "blues"],
        "taylor swift": ["pop", "country"],
        "drake": ["hip-hop", "rap"],
        "beyoncé": ["pop", "r&b"],
        "metallica": ["rock", "metal"],
        "bob marley": ["reggae"],
        "adele": ["pop", "soul"],
        "john legend": ["r&b", "soul"],
        "lil wayne": ["hip-hop", "rap"],
        "ariana grande": ["pop", "r&b"],
        "ed sheeran": ["pop", "folk"],
        "billie eilish": ["pop", "alternative"],
        "the weeknd": ["pop", "r&b"],
        "justin bieber": ["pop", "r&b"],
        "coldplay": ["rock", "pop"],
        "imagine dragons": ["rock", "pop"],
        "dua lipa": ["pop", "dance"],
        "calvin harris": ["electronic", "dance"],
        "marshmello": ["electronic", "dance"],
        "avicii": ["electronic", "dance"],
        "zedd": ["electronic", "dance"],
        "sia": ["pop", "electronic"],
        "bruno mars": ["pop", "funk"],
        "lady gaga": ["pop", "dance"],
        "rihanna": ["pop", "r&b"],
        "kendrick lamar": ["hip-hop", "rap"],
        "j cole": ["hip-hop", "rap"],
        "lorde": ["pop", "alternative"],
        "florence + the machine": ["rock", "indie"],
        "the lumineers": ["folk", "indie"],
        "mumford & sons": ["folk", "rock"],
        "lana del rey": ["pop", "indie"],
        "halsey": ["pop", "alternative"],       
        "sza": ["r&b", "pop"], 
        "doja cat": ["pop", "hip-hop"],
        "megan thee stallion": ["hip-hop", "rap"],
        "olivia rodrigo": ["pop", "rock"],
        "billie eilish": ["pop", "alternative"],
        "the chainsmokers": ["electronic", "pop"],
        "shawn mendes": ["pop", "rock"],
        "camila cabello": ["pop", "latin"],
    }
    
    # Check artist-based classification first (only if artist is provided)
    if artist_name and artist_lower in artist_genres:
        genres.extend(artist_genres[artist_lower])
    
    # Check keyword-based classification (works with title only or title + artist)
    for genre, keywords in genre_keywords.items():
        if any(keyword in text for keyword in keywords):
            if genre not in genres:
                genres.append(genre)
    
    # Additional pattern matching for better diversity
    if not genres:
        # Language/cultural patterns
        if any(word in text for word in ['de', 'la', 'el', 'con', 'por', 'para']):
            if "latin" not in genres:
                genres.append("latin")
        
        # Time/era references
        if any(word in text for word in ['80s', '90s', 'retro', 'vintage', 'classic']):
            if "rock" not in genres:
                genres.append("rock")
        
        # Instrument references
        if any(word in text for word in ['piano', 'guitar', 'drums', 'bass']):
            if "rock" not in genres:
                genres.append("rock")
        elif any(word in text for word in ['violin', 'cello', 'flute', 'harp']):
            if "classical" not in genres:
                genres.append("classical")
        elif any(word in text for word in ['saxophone', 'trumpet', 'trombone']):
            if "jazz" not in genres:
                genres.append("jazz")
        
        # Mood/energy patterns
        if any(word in text for word in ['chill', 'relax', 'calm', 'peaceful']):
            if "ambient" not in genres:
                genres.append("ambient")
        elif any(word in text for word in ['energy', 'power', 'loud', 'wild']):
            if "rock" not in genres:
                genres.append("rock")
    
    # Intelligent fallback with diversity - analyze title patterns
    if not genres:
        # Use length and character patterns to guess genre
        title_length = len(song_title)
        
        # Pattern-based fallback for diversity
        if any(char in title_lower for char in ['♪', '♫', '♩', '♬']):
            genres = ["classical"]
        elif any(num in title_lower for num in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0']):
            genres = ["electronic"]  # Numbers often in electronic/dance tracks
        elif any(word in title_lower for word in ['love', 'heart', 'baby', 'girl', 'boy']):
            genres = ["r&b"]  # Romantic themes
        elif any(word in title_lower for word in ['night', 'dark', 'shadow', 'black']):
            genres = ["rock"]  # Darker themes
        elif any(word in title_lower for word in ['sun', 'light', 'day', 'morning', 'bright']):
            genres = ["folk"]  # Brighter, nature themes
        elif any(word in title_lower for word in ['party', 'dance', 'club', 'floor']):
            genres = ["electronic"]  # Party themes
        elif any(word in title_lower for word in ['pain', 'cry', 'sad', 'tears', 'lonely']):
            genres = ["blues"]  # Emotional themes
        elif title_length > 50:  # Very long titles
            genres = ["progressive rock"]
        elif title_length < 10:  # Very short titles
            genres = ["punk"]
        elif '(' in title_lower and ')' in title_lower:  # Titles with parentheses
            genres = ["electronic"]  # Often remixes or versions
        else:
            # Final diverse fallback based on alphabetical distribution
            first_letter = title_lower[0] if title_lower else 'a'
            fallback_map = {
                'a': ["alternative"], 'b': ["blues"], 'c': ["country"], 'd': ["dance"],
                'e': ["electronic"], 'f': ["folk"], 'g': ["gospel"], 'h': ["hip-hop"],
                'i': ["indie"], 'j': ["jazz"], 'k': ["rock"], 'l': ["latin"],
                'm': ["metal"], 'n': ["r&b"], 'o': ["pop"], 'p': ["punk"],
                'q': ["rock"], 'r': ["reggae"], 's': ["soul"], 't': ["techno"],
                'u': ["underground"], 'v': ["rock"], 'w': ["world"], 'x': ["experimental"],
                'y': ["pop"], 'z': ["rock"]
            }
            genres = fallback_map.get(first_letter, ["indie"])
    
    return genres[:3]  # Limit to 3 genres max

def process_songs_with_ai(batch_size=100):
    """Process songs using AI/rule-based genre classification"""
    session = SessionLocal()
    
    try:
        # Get songs without genres
        songs_query = session.execute(text('''
            SELECT ds.song_id, ds.song_title, da.artist_name
            FROM dim_song ds
            JOIN dim_song_artist dsa ON ds.song_id = dsa.song_id
            JOIN dim_artist da ON dsa.artist_id = da.artist_id
            LEFT JOIN dim_song_genre dsg ON ds.song_id = dsg.song_id
            WHERE dsg.song_id IS NULL
            LIMIT :batch_size
        '''), {"batch_size": batch_size}).fetchall()
        
        print(f"Processing {len(songs_query)} songs with AI classification...")
        
        for song_id, song_title, artist_name in songs_query:
            print(f"Classifying: '{song_title}' by {artist_name}")
            
            # Get genres from AI/rules
            predicted_genres = classify_genres_with_llm(song_title, artist_name)
            
            # Save to database
            for genre_name in predicted_genres:
                # Get or create genre
                genre = session.query(DimGenre).filter_by(genre_name=genre_name).first()
                if not genre:
                    genre = DimGenre(genre_name=genre_name)
                    session.add(genre)
                    session.commit()
                
                # Link song to genre
                existing = session.query(DimSongGenre).filter_by(
                    song_id=song_id, genre_id=genre.genre_id
                ).first()
                if not existing:
                    link = DimSongGenre(song_id=song_id, genre_id=genre.genre_id)
                    session.add(link)
            
            session.commit()
            print(f"  -> Assigned genres: {predicted_genres}")
            
            # No delay needed for local processing - let's go fast!
            # time.sleep(0.1)
        
        print(f"Completed processing {len(songs_query)} songs!")
        
    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
    finally:
        session.close()

def process_songs_without_artists(batch_size=1000):
    """Process songs that don't have artist relationships using title-only classification"""
    session = SessionLocal()
    
    try:
        # Get songs without artist relationships and without genres
        songs_query = session.execute(text('''
            SELECT ds.song_id, ds.song_title
            FROM dim_song ds
            LEFT JOIN dim_song_artist dsa ON ds.song_id = dsa.song_id
            LEFT JOIN dim_song_genre dsg ON ds.song_id = dsg.song_id
            WHERE dsa.song_id IS NULL AND dsg.song_id IS NULL
            LIMIT :batch_size
        '''), {"batch_size": batch_size}).fetchall()
        
        print(f"Processing {len(songs_query)} songs WITHOUT artists using title-only classification...")
        
        for song_id, song_title in songs_query:
            print(f"Classifying (title-only): '{song_title}'")
            
            # Get genres from title-only rules
            predicted_genres = rule_based_genre_classification(song_title, artist_name=None)
            
            # Save to database
            for genre_name in predicted_genres:
                # Get or create genre
                genre = session.query(DimGenre).filter_by(genre_name=genre_name).first()
                if not genre:
                    genre = DimGenre(genre_name=genre_name)
                    session.add(genre)
                    session.commit()
                
                # Link song to genre
                existing = session.query(DimSongGenre).filter_by(
                    song_id=song_id, genre_id=genre.genre_id
                ).first()
                if not existing:
                    link = DimSongGenre(song_id=song_id, genre_id=genre.genre_id)
                    session.add(link)
            
            session.commit()
            print(f"  -> Assigned genres: {predicted_genres}")
        
        print(f"Completed processing {len(songs_query)} songs without artists!")
        
    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    # Scale up to process all remaining songs in batches
    process_songs_with_ai(batch_size=1000)  # Process 1000 songs at a time