#!/bin/bash
# Reduced Load Restart - Gentle on Spotify API
# Use after rate limiting recovery

echo "🔄 Starting REDUCED LOAD processing..."
echo "   - Only 3 processes (instead of 6)"
echo "   - 2 workers each (instead of 4)" 
echo "   - Smaller batches with delays"
echo

# Wait a bit between process starts
start_process() {
    local process_id=$1
    local min_id=$2
    local max_id=$3
    
    echo "Starting Process $process_id: range $min_id - $max_id"
    
    nohup python spotify_genres_optimized.py \
        --process_id $process_id \
        --min_id $min_id \
        --max_id $max_id \
        --max_workers 2 \
        --batch_size 15 \
        > speed_reduced_${process_id}.log 2>&1 &
    
    echo $! > speed_reduced_${process_id}.pid
    sleep 30  # 30 second delay between starts
}

# Start only 3 processes with larger ranges
start_process 0 1 210000
start_process 1 210001 420000  
start_process 2 420001 700000

echo "✅ Started 3 reduced-load processes"
echo "Monitor with: tail -f speed_reduced_*.log"
