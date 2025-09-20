#!/bin/bash

# Spotify Genre Processing Manager
# This script manages parallel processing of genre data across multiple terminals

set -e  # Exit on any error

# Configuration
TOTAL_SONGS=630000
NUM_PROCESSES=4  # Reduced from 8 to avoid rate limiting
SONGS_PER_PROCESS=$((TOTAL_SONGS / NUM_PROCESSES))

# Calculate ID ranges
RANGES=(
    "1 157500"
    "157501 315000" 
    "315001 472500"
    "472501 630000"
)

echo "=== Spotify Genre Processing Manager ==="
echo "Total songs: $TOTAL_SONGS"
echo "Number of processes: $NUM_PROCESSES"
echo "Songs per process: ~$SONGS_PER_PROCESS"
echo ""

# Function to run a single process
run_process() {
    local process_id=$1
    local min_id=$2
    local max_id=$3
    
    echo "Starting Process $process_id: IDs $min_id - $max_id"
    
    python spotify_genres_optimized.py \
        --min_id $min_id \
        --max_id $max_id \
        --process_id $process_id \
        --max_workers 3 \
        --batch_size 50 \
        > "process_${process_id}.log" 2>&1 &
    
    echo $! > "process_${process_id}.pid"
    echo "Process $process_id started with PID: $(cat process_${process_id}.pid)"
}

# Function to check if all processes are done
check_processes() {
    local all_done=true
    
    for i in $(seq 0 $((NUM_PROCESSES - 1))); do
        if [ -f "process_${i}.pid" ]; then
            local pid=$(cat "process_${i}.pid")
            if kill -0 $pid 2>/dev/null; then
                all_done=false
            else
                rm -f "process_${i}.pid"
                echo "Process $i completed"
            fi
        fi
    done
    
    echo $all_done
}

# Function to show progress
show_progress() {
    echo ""
    echo "=== Progress Report ==="
    local total_processed=0
    
    for i in $(seq 0 $((NUM_PROCESSES - 1))); do
        if [ -f "progress_${i}.json" ]; then
            local processed=$(python3 -c "
import json
try:
    with open('progress_${i}.json', 'r') as f:
        data = json.load(f)
        print(data.get('total_processed', 0))
except:
    print(0)
")
            echo "Process $i: $processed songs processed"
            total_processed=$((total_processed + processed))
        else
            echo "Process $i: No progress file found"
        fi
    done
    
    echo "Total processed: $total_processed / $TOTAL_SONGS"
    local percentage=$((total_processed * 100 / TOTAL_SONGS))
    echo "Overall progress: $percentage%"
    echo ""
}

# Function to cleanup
cleanup() {
    echo ""
    echo "=== Cleanup ==="
    for i in $(seq 0 $((NUM_PROCESSES - 1))); do
        if [ -f "process_${i}.pid" ]; then
            local pid=$(cat "process_${i}.pid")
            if kill -0 $pid 2>/dev/null; then
                echo "Terminating process $i (PID: $pid)"
                kill $pid
                sleep 2
                if kill -0 $pid 2>/dev/null; then
                    kill -9 $pid
                fi
            fi
            rm -f "process_${i}.pid"
        fi
    done
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Main execution
echo "Starting all processes..."
echo ""

# Start all processes
for i in $(seq 0 $((NUM_PROCESSES - 1))); do
    range=(${RANGES[$i]})
    min_id=${range[0]}
    max_id=${range[1]}
    
    run_process $i $min_id $max_id
    sleep 5  # Stagger starts to avoid initial rate limiting
done

echo ""
echo "All processes started. Monitoring progress..."
echo "Press Ctrl+C to stop all processes"
echo ""

# Monitor progress
while true; do
    if [ "$(check_processes)" = "true" ]; then
        echo "All processes completed!"
        show_progress
        break
    fi
    
    show_progress
    sleep 30  # Show progress every 30 seconds
done

echo ""
echo "=== Final Summary ==="
show_progress

# Merge cache files
echo "Merging cache files..."
python3 -c "
import pickle
import os

merged_cache = {}
for i in range($NUM_PROCESSES):
    cache_file = f'artist_genre_cache_{i}.pkl'
    if os.path.exists(cache_file):
        with open(cache_file, 'rb') as f:
            cache = pickle.load(f)
            merged_cache.update(cache)
        print(f'Merged cache from process {i}: {len(cache)} entries')

with open('artist_genre_cache_merged.pkl', 'wb') as f:
    pickle.dump(merged_cache, f)

print(f'Final merged cache: {len(merged_cache)} entries')
"

echo "Processing complete!"