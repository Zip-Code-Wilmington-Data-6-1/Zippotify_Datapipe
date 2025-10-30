#!/bin/bash
# Cleanup script to remove unnecessary files for streamlined FastAPI + data loading setup

echo "🧹 Cleaning up unnecessary files..."

# Remove Spotify API processing files
rm -f spotify_genres*.py
rm -f start_phase*.sh  
rm -f phase_monitor.py
rm -f artist_genre_cache_*.pkl
rm -f cooldown_calculator.py
rm -f emergency_recovery.py
rm -f recovery_*.py
rm -f smart_scaling.py
rm -f prewarm_cache.py

# Remove AI classification files  
rm -f *ai_classifier.py
rm -f reclassify_all.py
rm -f requirements.ai.txt
rm -f requirements.hybrid_ai.txt
rm -f hybrid_ai_cache.db
rm -f song_genre_preload.pkl

# Remove log files
rm -f *.log
rm -f *.pid
rm -f *_start_time.txt

# Remove monitoring scripts
rm -f progress_tracker.py
rm -f live_monitor.py
rm -f monitor_progress.py
rm -f quick_status.py
rm -f check_status.py
rm -f turbo_monitor.py

# Remove shell scripts
rm -f run_*.sh
rm -f quick_progress.sh

# Remove test files
rm -f test*.py
rm -f create_high_speed_setup.py

# Remove generated CSV files (data is in database)
rm -f *.csv

# Remove ETL folder if using load_tables.py instead
# rm -rf etl/

echo "✅ Cleanup complete! Your project now contains only essential files."
echo "📁 Core files remaining:"
echo "   - load_tables.py (data loading)"  
echo "   - fast_api.py (API endpoints)"
echo "   - database.py (DB connection)"
echo "   - models.py (data models)"
echo "   - .env (credentials)"
echo "   - requirements.txt (dependencies)"
echo "   - data/ and sample/ folders"