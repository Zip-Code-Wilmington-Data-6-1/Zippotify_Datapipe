#!/usr/bin/env python3
"""
Complete genre processing for remaining songs without artist relationships
Uses title-only classification to achieve 100% completion
"""

import time
from database import SessionLocal
from sqlalchemy import text
from ai_genre_classifier import process_songs_without_artists

def get_completion_stats():
    """Get current processing statistics"""
    session = SessionLocal()
    try:
        total_songs = session.execute(text('SELECT COUNT(*) FROM dim_song')).scalar()
        songs_with_genres = session.execute(text('SELECT COUNT(DISTINCT song_id) FROM dim_song_genre')).scalar()
        songs_without_artists = session.execute(text('''
            SELECT COUNT(*) FROM dim_song ds
            LEFT JOIN dim_song_artist dsa ON ds.song_id = dsa.song_id
            LEFT JOIN dim_song_genre dsg ON ds.song_id = dsg.song_id
            WHERE dsa.song_id IS NULL AND dsg.song_id IS NULL
        ''')).scalar()
        
        completion = (songs_with_genres / total_songs) * 100 if total_songs > 0 else 0
        
        return {
            'total_songs': total_songs,
            'processed': songs_with_genres,
            'remaining_no_artists': songs_without_artists,
            'completion_percentage': completion
        }
    finally:
        session.close()

def complete_remaining_songs():
    """Complete processing for all remaining songs without artists"""
    print("🎵 Completing Genre Processing for Songs Without Artists 🎵")
    print("=" * 60)
    
    session = SessionLocal()
    
    try:
        # Get initial stats
        stats = session.execute(text('''
            SELECT 
                COUNT(*) as total_songs,
                COUNT(DISTINCT dsg.song_id) as songs_with_genres,
                COUNT(*) - COUNT(DISTINCT dsg.song_id) as remaining_songs,
                COUNT(CASE WHEN dsa.song_id IS NULL THEN 1 END) as songs_without_artists
            FROM dim_song ds
            LEFT JOIN dim_song_artist dsa ON ds.song_id = dsa.song_id
            LEFT JOIN dim_song_genre dsg ON ds.song_id = dsg.song_id
        ''')).fetchone()
        
        total_songs, processed, remaining, no_artists = stats
        
        print(f"Initial Status:")
        print(f"  Total songs: {total_songs:,}")
        print(f"  Already processed: {processed:,}")
        print(f"  Songs without artists: {no_artists:,}")
        print(f"  Current completion: {(processed/total_songs)*100:.2f}%")
        print()
        
        if no_artists == 0:
            print("🎉 All songs are already processed!")
            return
        
        print(f"Processing {no_artists:,} songs without artist relationships...")
        print("Using title-only genre classification")
        print()
        
        start_time = time.time()
        batch_count = 0
        
        # Process in batches until complete
        while True:
            batch_count += 1
            batch_start = time.time()
            
            print(f"📦 Batch {batch_count} - Processing songs without artists...")
            
            # Process one batch
            process_songs_without_artists(batch_size=1000)
            
            # Check remaining
            remaining_count = session.execute(text('''
                SELECT COUNT(*) FROM dim_song ds
                LEFT JOIN dim_song_artist dsa ON ds.song_id = dsa.song_id
                LEFT JOIN dim_song_genre dsg ON ds.song_id = dsg.song_id
                WHERE dsa.song_id IS NULL AND dsg.song_id IS NULL
            ''')).scalar()
            
            batch_time = time.time() - batch_start
            total_time = time.time() - start_time
            
            # Get updated completion
            current_processed = session.execute(text('SELECT COUNT(DISTINCT song_id) FROM dim_song_genre')).scalar()
            completion = (current_processed / total_songs) * 100
            
            print(f"   Batch completed in {batch_time:.1f}s")
            print(f"   Progress: {current_processed:,}/{total_songs:,} ({completion:.2f}%)")
            print(f"   Remaining without artists: {remaining_count:,}")
            print()
            
            # Check if we're done
            if remaining_count == 0:
                print("🎉 All songs have been processed!")
                break
                
    except Exception as e:
        print(f"❌ Error during processing: {e}")
    
    finally:
        session.close()
        
        # Final stats
        final_stats = get_completion_stats()
        total_time = time.time() - start_time
        
        print("=" * 60)
        print("📊 Final Statistics:")
        print(f"  Total processing time: {total_time/60:.1f} minutes")
        print(f"  Songs processed: {final_stats['processed']:,}")
        print(f"  Final completion: {final_stats['completion_percentage']:.2f}%")
        
        if final_stats['completion_percentage'] >= 99.9:
            print("🎉 MISSION ACCOMPLISHED! 100% completion achieved!")
        else:
            print(f"  Remaining songs: {final_stats['remaining_no_artists']:,}")

if __name__ == "__main__":
    complete_remaining_songs()