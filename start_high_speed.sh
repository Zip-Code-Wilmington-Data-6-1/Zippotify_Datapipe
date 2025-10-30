#!/bin/bash

# High-Speed Processing Script
# Uses existing optimized script with aggressive settings

set -e

echo "🚀 HIGH-SPEED Spotify Genre Processing"
echo "Target: 24-48 hour completion"
echo "Prewarmed cache ready with 148 artists!"
echo ""

# Start 6 processes with optimized settings
echo "⚡ Starting 6 high-speed processes..."

# Process 0: 1-105000
python spotify_genres_optimized.py --min_id 1 --max_id 105000 --process_id 0 --max_workers 4 --batch_size 30 > speed_0.log 2>&1 &
echo $! > speed_0.pid
echo "✅ Process 0 started (1-105000) PID: $(cat speed_0.pid)"

sleep 5

# Process 1: 105001-210000  
python spotify_genres_optimized.py --min_id 105001 --max_id 210000 --process_id 1 --max_workers 4 --batch_size 30 > speed_1.log 2>&1 &
echo $! > speed_1.pid
echo "✅ Process 1 started (105001-210000) PID: $(cat speed_1.pid)"

sleep 5

# Process 2: 210001-315000
python spotify_genres_optimized.py --min_id 210001 --max_id 315000 --process_id 2 --max_workers 4 --batch_size 30 > speed_2.log 2>&1 &
echo $! > speed_2.pid
echo "✅ Process 2 started (210001-315000) PID: $(cat speed_2.pid)"

sleep 5

# Process 3: 315001-420000
python spotify_genres_optimized.py --min_id 315001 --max_id 420000 --process_id 3 --max_workers 4 --batch_size 30 > speed_3.log 2>&1 &
echo $! > speed_3.pid
echo "✅ Process 3 started (315001-420000) PID: $(cat speed_3.pid)"

sleep 5

# Process 4: 420001-525000
python spotify_genres_optimized.py --min_id 420001 --max_id 525000 --process_id 4 --max_workers 4 --batch_size 30 > speed_4.log 2>&1 &
echo $! > speed_4.pid
echo "✅ Process 4 started (420001-525000) PID: $(cat speed_4.pid)"

sleep 5

# Process 5: 525001-700000
python spotify_genres_optimized.py --min_id 525001 --max_id 700000 --process_id 5 --max_workers 4 --batch_size 30 > speed_5.log 2>&1 &
echo $! > speed_5.pid
echo "✅ Process 5 started (525001-700000) PID: $(cat speed_5.pid)"

echo ""
echo "🎯 All 6 HIGH-SPEED processes started!"
echo "📊 Total: 24 concurrent API calls (6 processes × 4 workers)"
echo "⚡ Expected: 600-1000 songs/hour with prewarmed cache"
echo "🎯 Target completion: 24-48 hours"
echo ""
echo "📈 Monitor progress with:"
echo "   python monitor_progress.py --processes 6"
echo ""
echo "📝 Check logs with:"
echo "   tail -f speed_*.log"
echo ""
echo "🛑 Stop all processes:"
echo "   python recovery_tool.py --stop"

# Record start time
date +%s > speed_start_time.txt
echo "⏱️  Started at: $(date)"