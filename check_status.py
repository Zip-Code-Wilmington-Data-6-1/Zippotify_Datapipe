#!/usr/bin/env python3
"""
Quick status check for processed songs in the database
"""

import os
import sys
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, text
from models import DimSong, DimGenre, DimSongGenre
from database import SessionLocal

def check_processed_status():
    """Check how many songs are already processed"""
    session = SessionLocal()
    
    try:
        print("🎵 Spotify Genre Processing Status Check")
        print("=" * 50)
        
        # Total songs in database
        total_songs = session.query(DimSong).count()
        print(f"Total songs in database: {total_songs:,}")
        
        # Songs with genres
        songs_with_genres = session.query(DimSongGenre.song_id).distinct().count()
        print(f"Songs with genres: {songs_with_genres:,}")
        
        # Percentage processed
        if total_songs > 0:
            percentage = (songs_with_genres / total_songs) * 100
            print(f"Processing progress: {percentage:.1f}%")
        
        print()
        
        # Genre statistics
        total_genres = session.query(DimGenre).count()
        print(f"Total unique genres: {total_genres:,}")
        
        # Total genre-song relationships
        total_relationships = session.query(DimSongGenre).count()
        print(f"Total song-genre relationships: {total_relationships:,}")
        
        if songs_with_genres > 0:
            avg_genres_per_song = total_relationships / songs_with_genres
            print(f"Average genres per song: {avg_genres_per_song:.1f}")
        
        print()
        
        # Range analysis for the proposed splits
        ranges = [
            (1, 157500),
            (157501, 315000), 
            (315001, 472500),
            (472501, 630000)
        ]
        
        print("📊 Processing Status by Range:")
        print("-" * 50)
        print(f"{'Range':<20} {'Total':<8} {'Processed':<10} {'Remaining':<10} {'%Done':<8}")
        print("-" * 50)
        
        overall_processed = 0
        overall_total = 0
        
        for i, (min_id, max_id) in enumerate(ranges):
            # Count songs in range
            range_total = session.query(DimSong).filter(
                and_(DimSong.song_id >= min_id, DimSong.song_id <= max_id)
            ).count()
            
            # Count processed songs in range
            range_processed = session.query(DimSong.song_id).join(
                DimSongGenre, DimSong.song_id == DimSongGenre.song_id
            ).filter(
                and_(DimSong.song_id >= min_id, DimSong.song_id <= max_id)
            ).distinct().count()
            
            range_remaining = range_total - range_processed
            range_percentage = (range_processed / range_total * 100) if range_total > 0 else 0
            
            overall_total += range_total
            overall_processed += range_processed
            
            print(f"Range {i+1:<2} ({min_id}-{max_id}) {range_total:<8,} {range_processed:<10,} {range_remaining:<10,} {range_percentage:<7.1f}%")
        
        print("-" * 50)
        overall_percentage = (overall_processed / overall_total * 100) if overall_total > 0 else 0
        overall_remaining = overall_total - overall_processed
        print(f"{'TOTAL':<20} {overall_total:<8,} {overall_processed:<10,} {overall_remaining:<10,} {overall_percentage:<7.1f}%")
        
        print()
        
        # Estimate processing time
        if overall_remaining > 0:
            # Assuming 3 API calls per song (search + 2 artist lookups average)
            # With 4 processes × 3 workers = 12 concurrent calls
            # Rate limited to ~80 calls per minute total
            calls_per_song = 3
            total_calls_needed = overall_remaining * calls_per_song
            calls_per_minute = 80
            
            estimated_minutes = total_calls_needed / calls_per_minute
            estimated_hours = estimated_minutes / 60
            estimated_days = estimated_hours / 24
            
            print("⏱️  Estimated Processing Time:")
            print(f"   API calls needed: {total_calls_needed:,}")
            print(f"   Estimated time: {estimated_minutes:.0f} minutes ({estimated_hours:.1f} hours, {estimated_days:.1f} days)")
            print()
            print("   Note: This is a conservative estimate assuming rate limiting.")
            print("   Actual time may be faster due to caching and fewer API calls per song.")
        
        print()
        
        # Most common genres (if any exist)
        if total_genres > 0:
            print("🎯 Top 10 Most Common Genres:")
            top_genres = session.query(
                DimGenre.genre_name,
                func.count(DimSongGenre.song_id).label('song_count')
            ).join(
                DimSongGenre, DimGenre.genre_id == DimSongGenre.genre_id
            ).group_by(
                DimGenre.genre_name
            ).order_by(
                func.count(DimSongGenre.song_id).desc()
            ).limit(10).all()
            
            for i, (genre_name, count) in enumerate(top_genres, 1):
                print(f"   {i:2}. {genre_name:<20} ({count:,} songs)")
        
    except Exception as e:
        print(f"Error checking status: {e}")
        return False
    finally:
        session.close()
    
    return True

def main():
    print("Checking database connection...")
    
    try:
        session = SessionLocal()
        session.execute(text("SELECT 1"))
        session.close()
        print("✅ Database connection successful!")
        print()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("Make sure PostgreSQL is running and the database is accessible.")
        return
    
    check_processed_status()

if __name__ == "__main__":
    main()