#!/bin/bash

# High-Speed Spotify Genre Processing Manager
# Optimized for 24-48 hour completion

set -e

# Aggressive Configuration for Speed
TOTAL_SONGS=630000
NUM_PROCESSES=6  # Double the processes
SONGS_PER_PROCESS=$((TOTAL_SONGS / NUM_PROCESSES))

# Smaller, more manageable ranges for better distribution
RANGES=(
    "1 105000"
    "105001 210000" 
    "210001 315000"
    "315001 420000"
    "420001 525000"
    "525001 630000"
)

echo "=== HIGH-SPEED Spotify Genre Processing Manager ==="
echo "🎯 TARGET: 24-48 hours completion"
echo "Total songs: $TOTAL_SONGS"
echo "Number of processes: $NUM_PROCESSES"
echo "Songs per process: ~$SONGS_PER_PROCESS"
echo ""
echo "⚡ SPEED FEATURES:"
echo "  🔥 6 parallel processes instead of 3"
echo "  🚀 4 workers per process (24 total API calls max)"
echo "  ⚡ Smaller batch sizes for faster feedback"
echo "  🎯 Aggressive but safe rate limiting"
echo ""

# Function to run a high-speed process
run_speed_process() {
    local process_id=$1
    local min_id=$2
    local max_id=$3
    
    echo "⚡ Starting Speed Process $process_id: IDs $min_id - $max_id"
    
    python spotify_genres_optimized.py \
        --min_id $min_id \
        --max_id $max_id \
        --process_id $process_id \
        --max_workers 4 \
        --batch_size 30 \
        > "speed_process_${process_id}.log" 2>&1 &
    
    echo $! > "speed_process_${process_id}.pid"
    echo "✅ Speed Process $process_id started with PID: $(cat speed_process_${process_id}.pid)"
}

# Function to check process health with faster recovery
check_speed_process_health() {
    local process_id=$1
    local pid_file="speed_process_${process_id}.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 $pid 2>/dev/null; then
            # Check if process is stuck (no log updates in 5 minutes instead of 10)
            local log_file="speed_process_${process_id}.log"
            if [ -f "$log_file" ]; then
                local last_update=$(stat -f %m "$log_file" 2>/dev/null || echo 0)
                local now=$(date +%s)
                local diff=$((now - last_update))
                
                if [ $diff -gt 300 ]; then  # 5 minutes
                    echo "⚠️  Speed Process $process_id appears stuck (no updates for ${diff}s)"
                    return 2  # Stuck
                fi
            fi
            return 0  # Running
        else
            rm -f "$pid_file"
            return 1  # Stopped
        fi
    fi
    return 1  # Not started or stopped
}

# Function to restart a stuck process faster
restart_speed_process() {
    local process_id=$1
    local range=(${RANGES[$process_id]})
    local min_id=${range[0]}
    local max_id=${range[1]}
    
    echo "🔄 FAST RESTART: Speed process $process_id..."
    
    # Kill the stuck process
    local pid_file="speed_process_${process_id}.pid"
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 $pid 2>/dev/null; then
            kill -KILL $pid  # More aggressive kill
        fi
        rm -f "$pid_file"
    fi
    
    # Shorter wait before restart
    sleep 5
    
    # Restart the process
    run_speed_process $process_id $min_id $max_id
}

# Function to check if all processes are done
check_speed_processes() {
    local all_done=true
    local running_count=0
    local stuck_count=0
    
    for i in $(seq 0 $((NUM_PROCESSES - 1))); do
        local status=$(check_speed_process_health $i)
        case $status in
            0) # Running
                all_done=false
                running_count=$((running_count + 1))
                ;;
            1) # Stopped
                echo "✅ Speed Process $i completed"
                ;;
            2) # Stuck
                all_done=false
                stuck_count=$((stuck_count + 1))
                restart_speed_process $i
                ;;
        esac
    done
    
    echo "⚡ Speed Status: $running_count running, $stuck_count restarted"
    echo $all_done
}

# Function to show enhanced speed progress
show_speed_progress() {
    echo ""
    echo "=== 🚀 HIGH-SPEED Progress Report ==="
    local total_processed=0
    local total_rate=0
    
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
            echo "⚡ Speed Process $i: $processed songs completed"
            total_processed=$((total_processed + processed))
        else
            echo "⚡ Speed Process $i: Initializing..."
        fi
    done
    
    echo ""
    echo "🎯 SPEED Statistics:"
    echo "   Total processed: $total_processed / $TOTAL_SONGS"
    local percentage=$((total_processed * 100 / TOTAL_SONGS))
    echo "   Overall progress: $percentage%"
    
    # Calculate processing rate
    if [ $total_processed -gt 0 ]; then
        local start_time_file="speed_start_time.txt"
        if [ ! -f "$start_time_file" ]; then
            date +%s > "$start_time_file"
        fi
        
        local start_time=$(cat "$start_time_file")
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        if [ $elapsed -gt 0 ]; then
            local rate=$((total_processed * 3600 / elapsed))  # songs per hour
            local remaining=$((TOTAL_SONGS - total_processed))
            local eta_hours=$((remaining / rate))
            
            echo "   🚀 Current rate: $rate songs/hour"
            echo "   ⏱️  ETA: $eta_hours hours"
            
            if [ $eta_hours -lt 48 ]; then
                echo "   🎯 ON TARGET for 24-48 hour completion!"
            else
                echo "   ⚠️  May need more optimization"
            fi
        fi
    fi
    
    # Progress bar
    local bar_width=50
    local filled_width=$((bar_width * percentage / 100))
    local bar=""
    for ((j=0; j<filled_width; j++)); do bar+="█"; done
    for ((j=filled_width; j<bar_width; j++)); do bar+="░"; done
    echo "   Progress: [$bar] $percentage%"
    echo ""
}

# Function to cleanup
cleanup_speed() {
    echo ""
    echo "=== 🧹 Speed Cleanup ==="
    for i in $(seq 0 $((NUM_PROCESSES - 1))); do
        if [ -f "speed_process_${i}.pid" ]; then
            local pid=$(cat "speed_process_${i}.pid")
            if kill -0 $pid 2>/dev/null; then
                echo "🛑 Terminating speed process $i (PID: $pid)"
                kill -TERM $pid
                sleep 2
                if kill -0 $pid 2>/dev/null; then
                    kill -KILL $pid
                fi
            fi
            rm -f "speed_process_${i}.pid"
        fi
    done
}

# Set up signal handlers
trap cleanup_speed SIGINT SIGTERM

# Record start time
date +%s > "speed_start_time.txt"

# Main execution
echo "🚀 Starting HIGH-SPEED processing for 24-48 hour completion..."
echo ""

# Start all processes with minimal delay
for i in $(seq 0 $((NUM_PROCESSES - 1))); do
    range=(${RANGES[$i]})
    min_id=${range[0]}
    max_id=${range[1]}
    
    run_speed_process $i $min_id $max_id
    sleep 8  # Shorter delay for faster startup
done

echo ""
echo "⚡ All HIGH-SPEED processes started!"
echo "🎯 TARGET: Complete in 24-48 hours"
echo "⏱️  Press Ctrl+C to stop all processes"
echo ""

# Speed monitoring loop
monitoring_cycle=0
while true; do
    monitoring_cycle=$((monitoring_cycle + 1))
    
    if [ "$(check_speed_processes)" = "true" ]; then
        echo "🎉 HIGH-SPEED processing completed!"
        show_speed_progress
        break
    fi
    
    show_speed_progress
    
    # Check every 20 seconds for faster response
    sleep 20
done

echo ""
echo "=== 🏁 HIGH-SPEED Final Summary ==="
show_speed_progress

# Merge cache files
echo "🔄 Merging HIGH-SPEED cache files..."
python3 -c "
import pickle
import os
import json

merged_cache = {}
total_processed = 0

for i in range($NUM_PROCESSES):
    cache_file = f'artist_genre_cache_{i}.pkl'
    if os.path.exists(cache_file):
        with open(cache_file, 'rb') as f:
            cache = pickle.load(f)
            merged_cache.update(cache)
        print(f'✅ Merged cache from speed process {i}: {len(cache)} entries')
    
    progress_file = f'progress_{i}.json'
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            progress = json.load(f)
            total_processed += progress.get('total_processed', 0)

with open('artist_genre_cache_speed_merged.pkl', 'wb') as f:
    pickle.dump(merged_cache, f)

# Calculate final stats
end_time = $(date +%s)
start_time = int(open('speed_start_time.txt').read().strip())
total_hours = (end_time - start_time) / 3600

print(f'')
print(f'🎯 HIGH-SPEED Results:')
print(f'   Total processed: {total_processed:,} songs')
print(f'   Total time: {total_hours:.1f} hours')
print(f'   Average rate: {total_processed/total_hours:.0f} songs/hour')
print(f'   Final cache size: {len(merged_cache):,} entries')
"

echo ""
echo "🎉 HIGH-SPEED processing complete!"