#!/bin/bash
# PHASE 2: Moderate Scale
# Scale up if Phase 1 was successful

echo "🚀 PHASE 2: Moderate Scale"
echo "   Target: 8,000 songs/hour"
echo "   3 processes × 4 workers = 12 concurrent"
echo

# Check Phase 1 results first
if grep -q "rate/request limit" phase1_*.log 2>/dev/null; then
    echo "❌ Phase 1 had rate limits - cannot proceed to Phase 2"
    echo "   Recommend staying with Phase 1 settings"
    exit 1
fi

echo "✅ Phase 1 clean - proceeding with Phase 2"

# Stop Phase 1 processes
for pid_file in phase1_*.pid; do
    if [ -f "$pid_file" ]; then
        kill $(cat "$pid_file") 2>/dev/null
        rm "$pid_file"
    fi
done

sleep 30

echo $(date +%s) > phase2_start_time.txt

start_phase2_process() {
    local process_id=$1
    local min_id=$2
    local max_id=$3
    
    echo "Starting Phase 2 Process $process_id: range $min_id-$max_id"
    
    nohup python spotify_genres_optimized.py \
        --process_id $process_id \
        --min_id $min_id \
        --max_id $max_id \
        --max_workers 4 \
        --batch_size 20 \
        > phase2_${process_id}.log 2>&1 &
    
    echo $! > phase2_${process_id}.pid
    sleep 45  # 45 seconds between starts  
}

# Start 3 processes
start_phase2_process 0 1 210000
start_phase2_process 1 210001 420000
start_phase2_process 2 420001 630000

echo "✅ Phase 2 started - 3 processes with 4 workers each"
echo "📊 Monitor: tail -f phase2_*.log"
