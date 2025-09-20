#!/bin/bash
# GENTLE RESTART - Ultra Conservative Approach
# Designed to avoid rate limiting completely

echo "🔄 GENTLE RESTART: Ultra-conservative approach"
echo "   Target: Stable 2,000-4,000 songs/hour (no rate limits)"
echo "   Strategy: Better slow and steady than fast and stuck"
echo

# Function to start a single process with conservative settings
start_gentle_process() {
    local process_id=$1
    local min_id=$2
    local max_id=$3
    local delay=$4
    
    echo "Starting Process $process_id: range $min_id-$max_id (delay: ${delay}s)"
    
    nohup python spotify_genres_optimized.py \
        --process_id $process_id \
        --min_id $min_id \
        --max_id $max_id \
        --max_workers 2 \
        --batch_size 10 \
        > gentle_${process_id}.log 2>&1 &
    
    echo $! > gentle_${process_id}.pid
    sleep $delay
}

# Test single process first
echo "Phase 1: Testing with single process..."
start_gentle_process 0 1 50000 0

echo "   Waiting 5 minutes to test API response..."
sleep 300

# Check if first process is working without rate limits
if grep -q "rate/request limit" gentle_0.log 2>/dev/null; then
    echo "❌ Still getting rate limits - STOP EVERYTHING"
    kill $(cat gentle_0.pid 2>/dev/null) 2>/dev/null
    echo "   Wait longer before restart (1-2 hours recommended)"
    exit 1
fi

echo "✅ Phase 1 successful - no immediate rate limits"
echo "Phase 2: Adding second process..."
start_gentle_process 1 50001 400000 60

sleep 300

echo "Phase 3: Adding third process if all good..."
if ! grep -q "rate/request limit" gentle_*.log 2>/dev/null; then
    start_gentle_process 2 400001 700000 60
    echo "✅ All 3 gentle processes started"
else
    echo "⚠️  Detected rate limits - staying with 2 processes"
fi

echo
echo "📊 Monitor progress:"
echo "tail -f gentle_*.log"
echo "python progress_tracker.py  # (will need updating for gentle_*.log)"
