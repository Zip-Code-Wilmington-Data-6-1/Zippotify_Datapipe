#!/usr/bin/env python3
"""
Full-scale genre processing using rule-based classification
Processes all 58,000+ songs efficiently
"""

import time
from database import SessionLocal
from sqlalchemy import text
from ai_genre_classifier import process_songs_with_ai

def get_processing_stats():
    """Get current processing statistics"""
    session = SessionLocal()
    try:
        total_songs = session.execute(text('SELECT COUNT(*) FROM dim_song')).scalar()
        songs_with_genres = session.execute(text('SELECT COUNT(DISTINCT song_id) FROM dim_song_genre')).scalar()
        total_genres = session.execute(text('SELECT COUNT(*) FROM dim_genre')).scalar()
        total_relationships = session.execute(text('SELECT COUNT(*) FROM dim_song_genre')).scalar()
        
        remaining = total_songs - songs_with_genres
        percentage = (songs_with_genres / total_songs) * 100 if total_songs > 0 else 0
        
        return {
            'total_songs': total_songs,
            'processed': songs_with_genres,
            'remaining': remaining,
            'percentage': percentage,
            'total_genres': total_genres,
            'total_relationships': total_relationships
        }
    finally:
        session.close()

def run_full_scale_processing():
    """Run full-scale genre processing with progress tracking"""
    print("🎵 Starting Full-Scale Genre Processing 🎵")
    print("=" * 50)
    
    start_time = time.time()
    batch_size = 1000
    batch_count = 0
    
    # Initial stats
    stats = get_processing_stats()
    print(f"Initial Status:")
    print(f"  Total songs: {stats['total_songs']:,}")
    print(f"  Already processed: {stats['processed']:,}")
    print(f"  Remaining: {stats['remaining']:,}")
    print(f"  Progress: {stats['percentage']:.2f}%")
    print()
    
    if stats['remaining'] == 0:
        print("🎉 All songs already processed!")
        return
    
    print(f"Processing {stats['remaining']:,} songs in batches of {batch_size:,}...")
    print("Press Ctrl+C to stop processing safely\n")
    
    try:
        while True:
            batch_start = time.time()
            batch_count += 1
            
            # Process one batch
            print(f"📦 Batch {batch_count} - Processing {batch_size:,} songs...")
            process_songs_with_ai(batch_size=batch_size)
            
            # Get updated stats
            stats = get_processing_stats()
            batch_time = time.time() - batch_start
            total_time = time.time() - start_time
            
            # Calculate rates
            songs_per_second = batch_size / batch_time if batch_time > 0 else 0
            songs_per_hour = songs_per_second * 3600
            
            # Estimate remaining time
            if songs_per_second > 0 and stats['remaining'] > 0:
                eta_seconds = stats['remaining'] / songs_per_second
                eta_minutes = eta_seconds / 60
                eta_hours = eta_minutes / 60
            else:
                eta_seconds = eta_minutes = eta_hours = 0
            
            print(f"✅ Batch {batch_count} completed in {batch_time:.1f}s")
            print(f"   Rate: {songs_per_second:.1f} songs/sec ({songs_per_hour:.0f} songs/hour)")
            print(f"   Progress: {stats['processed']:,}/{stats['total_songs']:,} ({stats['percentage']:.2f}%)")
            print(f"   Remaining: {stats['remaining']:,} songs")
            if eta_hours > 0:
                if eta_hours >= 1:
                    print(f"   ETA: {eta_hours:.1f} hours")
                else:
                    print(f"   ETA: {eta_minutes:.1f} minutes")
            print(f"   Genres created: {stats['total_genres']:,}")
            print()
            
            # Check if we're done
            if stats['remaining'] == 0:
                print("🎉 All songs processed successfully!")
                break
                
    except KeyboardInterrupt:
        print("\n⏸️  Processing stopped by user")
        stats = get_processing_stats()
        print(f"Current progress: {stats['processed']:,}/{stats['total_songs']:,} ({stats['percentage']:.2f}%)")
        print("You can resume processing later by running this script again.")
    
    except Exception as e:
        print(f"\n❌ Error during processing: {e}")
        stats = get_processing_stats()
        print(f"Current progress: {stats['processed']:,}/{stats['total_songs']:,} ({stats['percentage']:.2f}%)")
        print("You can resume processing later by running this script again.")
    
    finally:
        total_time = time.time() - start_time
        final_stats = get_processing_stats()
        
        print("\n" + "=" * 50)
        print("📊 Final Statistics:")
        print(f"  Total processing time: {total_time/3600:.1f} hours")
        print(f"  Songs processed: {final_stats['processed']:,}")
        print(f"  Genres created: {final_stats['total_genres']:,}")
        print(f"  Song-genre relationships: {final_stats['total_relationships']:,}")
        print(f"  Completion: {final_stats['percentage']:.2f}%")
        
        if batch_count > 0:
            avg_batch_time = total_time / batch_count
            avg_rate = batch_size / avg_batch_time if avg_batch_time > 0 else 0
            print(f"  Average rate: {avg_rate:.1f} songs/sec ({avg_rate*3600:.0f} songs/hour)")

if __name__ == "__main__":
    run_full_scale_processing()