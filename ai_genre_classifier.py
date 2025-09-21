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
    """Rule-based genre classification as fallback/demo"""
    genres = []
    
    # Convert to lowercase for matching
    title_lower = song_title.lower()
    artist_lower = artist_name.lower() if artist_name else ""
    text = f"{title_lower} {artist_lower}".strip()
    
    # Genre keywords and patterns
    genre_keywords = {
        "hip-hop": ["rap", "hip hop", "feat.", "nigga", "gangsta", "trap"],
        "rock": ["rock", "metal", "punk", "guitar"],
        "electronic": ["electronic", "techno", "house", "dance", "remix", "mix"],
        "country": ["country", "nashville", "cowboy", "rural"],
        "jazz": ["jazz", "blues", "swing"],
        "classical": ["classical", "symphony", "orchestra", "piano", "violin"],
        "pop": ["pop", "radio", "hit", "chart"],
        "r&b": ["r&b", "soul", "motown"],
        "reggae": ["reggae", "jamaica", "rasta"],
        "folk": ["folk", "acoustic", "traditional"],
        "gospel": ["gospel", "church", "praise", "worship", "jesus", "god"],
        "latin": ["latin", "spanish", "salsa", "mariachi"],
        "soundtrack": ["soundtrack", "theme", "score", "movie", "film"],
        "ambient": ["ambient", "chill", "meditation"],
        "indie": ["indie", "independent", "underground"]
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
        "prophet posse": ["hip-hop", "rap"]
    }
    
    # Check artist-based classification first (only if artist is provided)
    if artist_name and artist_lower in artist_genres:
        genres.extend(artist_genres[artist_lower])
    
    # Check keyword-based classification (works with title only or title + artist)
    for genre, keywords in genre_keywords.items():
        if any(keyword in text for keyword in keywords):
            if genre not in genres:
                genres.append(genre)
    
    # Default fallback
    if not genres:
        genres = ["pop"]  # Default genre
    
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