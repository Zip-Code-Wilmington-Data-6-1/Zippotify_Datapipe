#!/bin/bash
# PHASE 1: Conservative Test (2 hours)
# Test if we can avoid rate limits with moderate load

echo "🧪 PHASE 1: Conservative Test"
echo "   Target: 4,000 songs/hour for 2 hours"
echo "   If successful → proceed to Phase 2"
echo

# Store start time for phase tracking
echo $(date +%s) > phase1_start_time.txt

start_phase1_process() {
    local process_id=$1
    local min_id=$2
    local max_id=$3
    
    echo "Starting Phase 1 Process $process_id: range $min_id-$max_id"
    
    nohup python spotify_genres_optimized.py \
        --process_id $process_id \
        --min_id $min_id \
        --max_id $max_id \
        --max_workers 3 \
        --batch_size 15 \
        > phase1_${process_id}.log 2>&1 &
    
    echo $! > phase1_${process_id}.pid
    sleep 60  # 1 minute between starts
}

# Start 2 processes with conservative settings
start_phase1_process 0 1 315000
start_phase1_process 1 315001 630000

echo "✅ Phase 1 started - 2 processes with 3 workers each"
echo "📊 Monitor: tail -f phase1_*.log"
echo "⏰ Run for 2 hours, then check for rate limits"
echo "🚦 If no rate limits: ./start_phase2.sh"
