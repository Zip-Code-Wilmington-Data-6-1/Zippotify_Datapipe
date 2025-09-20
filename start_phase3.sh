#!/bin/bash
# PHASE 3: Target Scale
# Maximum safe scale for fastest completion

echo "🎯 PHASE 3: Target Scale"
echo "   Target: 12,000+ songs/hour"
echo "   4 processes × 4 workers = 16 concurrent"
echo

# Check Phase 2 results
if grep -q "rate/request limit" phase2_*.log 2>/dev/null; then
    echo "❌ Phase 2 had rate limits - cannot proceed to Phase 3"
    echo "   Staying with Phase 2 settings"
    exit 1
fi

echo "✅ Phase 2 clean - proceeding with Phase 3 (MAXIMUM SCALE)"

# Stop Phase 2 processes  
for pid_file in phase2_*.pid; do
    if [ -f "$pid_file" ]; then
        kill $(cat "$pid_file") 2>/dev/null
        rm "$pid_file"
    fi
done

sleep 45

echo $(date +%s) > phase3_start_time.txt

start_phase3_process() {
    local process_id=$1
    local min_id=$2
    local max_id=$3
    
    echo "Starting Phase 3 Process $process_id: range $min_id-$max_id"
    
    nohup python spotify_genres_optimized.py \
        --process_id $process_id \
        --min_id $min_id \
        --max_id $max_id \
        --max_workers 4 \
        --batch_size 25 \
        > phase3_${process_id}.log 2>&1 &
    
    echo $! > phase3_${process_id}.pid
    sleep 30  # 30 seconds between starts
}

# Start 4 processes for maximum throughput
start_phase3_process 0 1 157500
start_phase3_process 1 157501 315000  
start_phase3_process 2 315001 472500
start_phase3_process 3 472501 630000

echo "✅ Phase 3 started - 4 processes with 4 workers each"
echo "📊 Monitor: tail -f phase3_*.log"
echo "🎯 This should achieve 2-3 day completion target!"
