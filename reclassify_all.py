#!/usr/bin/env python3
"""
Reclassify all songs after clearing the genre table
"""

import time
import json
from database import SessionLocal
from sqlalchemy import text
from models import DimGenre, DimSongGenre
import os
from dotenv import load_dotenv

# Import the classification function from the main classifier
from ai_genre_classifier import rule_based_genre_classification

load_dotenv()

def reclassify_all_songs():
    """Reclassify ALL songs from scratch"""
    session = SessionLocal()
    
    try:
        # Get unique songs with their artists (concatenated for multi-artist songs)
        songs_query = session.execute(text('''
            SELECT ds.song_id, ds.song_title, 
                   STRING_AGG(da.artist_name, ', ' ORDER BY da.artist_name) as all_artists
            FROM dim_song ds
            JOIN dim_song_artist dsa ON ds.song_id = dsa.song_id
            JOIN dim_artist da ON dsa.artist_id = da.artist_id
            GROUP BY ds.song_id, ds.song_title
            ORDER BY ds.song_id
        ''')).fetchall()
        
        print(f"🎯 Processing {len(songs_query)} songs with artists...")
        processed = 0
        
        for song_id, song_title, all_artists in songs_query:
            if processed % 1000 == 0:
                print(f"Progress: {processed}/{len(songs_query)} songs processed ({processed/len(songs_query)*100:.1f}%)")
            
            # Get genres from the improved classifier
            predicted_genres = rule_based_genre_classification(song_title, all_artists)
            
            # Save to database
            for genre_name in predicted_genres:
                # Get or create genre
                genre = session.query(DimGenre).filter_by(genre_name=genre_name).first()
                if not genre:
                    genre = DimGenre(genre_name=genre_name)
                    session.add(genre)
                    session.commit()
                
                # Link song to genre (no need to check existing since table is empty)
                link = DimSongGenre(song_id=song_id, genre_id=genre.genre_id)
                session.add(link)
            
            # Commit every 100 songs for better performance
            if processed % 100 == 0:
                session.commit()
            
            processed += 1
        
        # Final commit
        session.commit()
        print(f"✅ Completed processing {processed} songs with artists!")
        
        # Now process songs WITHOUT artists
        songs_no_artist = session.execute(text('''
            SELECT ds.song_id, ds.song_title
            FROM dim_song ds
            LEFT JOIN dim_song_artist dsa ON ds.song_id = dsa.song_id
            WHERE dsa.song_id IS NULL
        ''')).fetchall()
        
        print(f"🎯 Processing {len(songs_no_artist)} songs without artists...")
        processed_no_artist = 0
        
        for song_id, song_title in songs_no_artist:
            if processed_no_artist % 1000 == 0:
                print(f"Progress: {processed_no_artist}/{len(songs_no_artist)} songs without artists processed ({processed_no_artist/len(songs_no_artist)*100:.1f}%)")
            
            # Get genres from title-only classification
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
                link = DimSongGenre(song_id=song_id, genre_id=genre.genre_id)
                session.add(link)
            
            # Commit every 100 songs
            if processed_no_artist % 100 == 0:
                session.commit()
            
            processed_no_artist += 1
        
        # Final commit
        session.commit()
        print(f"✅ Completed processing {processed_no_artist} songs without artists!")
        
        print(f"\n🎉 RECLASSIFICATION COMPLETE!")
        print(f"  - Songs with artists: {processed}")
        print(f"  - Songs without artists: {processed_no_artist}")
        print(f"  - Grand total: {processed + processed_no_artist}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    reclassify_all_songs()