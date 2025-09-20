#!/bin/bash

# Ultra-Optimized Spotify Genre Processing Manager
# Advanced rate limiting, circuit breakers, and intelligent retry strategies

set -e

# Configuration
TOTAL_SONGS=630000
NUM_PROCESSES=3  # Reduced for better rate limit management
SONGS_PER_PROCESS=$((TOTAL_SONGS / NUM_PROCESSES))

# More conservative ranges for better rate limit distribution
RANGES=(
    "1 233333"
    "233334 466666" 
    "466667 700000"
)

echo "=== Ultra-Optimized Spotify Genre Processing Manager ==="
echo "Total songs: $TOTAL_SONGS"
echo "Number of processes: $NUM_PROCESSES"
echo "Songs per process: ~$SONGS_PER_PROCESS"
echo ""
echo "🚀 Features:"
echo "  ✅ Circuit breakers for API failures"
echo "  ✅ Adaptive rate limiting with response header analysis"
echo "  ✅ Intelligent exponential backoff with jitter"
echo "  ✅ Granular checkpointing and resume"
echo "  ✅ Advanced caching and failure tracking"
echo "  ✅ Real-time statistics and monitoring"
echo ""

# Function to run a single process
run_optimized_process() {
    local process_id=$1
    local min_id=$2
    local max_id=$3
    
    echo "🎵 Starting Optimized Process $process_id: IDs $min_id - $max_id"
    
    python spotify_genres_ultra_optimized.py \
        --min_id $min_id \
        --max_id $max_id \
        --process_id $process_id \
        --max_workers 2 \
        --batch_size 20 \
        > "optimized_process_${process_id}.log" 2>&1 &
    
    echo $! > "optimized_process_${process_id}.pid"
    echo "✅ Process $process_id started with PID: $(cat optimized_process_${process_id}.pid)"
}

# Function to check process health
check_process_health() {
    local process_id=$1
    local pid_file="optimized_process_${process_id}.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 $pid 2>/dev/null; then
            # Check if process is stuck (no log updates in 10 minutes)
            local log_file="optimized_process_${process_id}.log"
            if [ -f "$log_file" ]; then
                local last_update=$(stat -f %m "$log_file" 2>/dev/null || echo 0)
                local now=$(date +%s)
                local diff=$((now - last_update))
                
                if [ $diff -gt 600 ]; then  # 10 minutes
                    echo "⚠️  Process $process_id appears stuck (no log updates for ${diff}s)"
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

# Function to restart a stuck process
restart_process() {
    local process_id=$1
    local range=(${RANGES[$process_id]})
    local min_id=${range[0]}
    local max_id=${range[1]}
    
    echo "🔄 Restarting stuck process $process_id..."
    
    # Kill the stuck process
    local pid_file="optimized_process_${process_id}.pid"
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 $pid 2>/dev/null; then
            kill -TERM $pid
            sleep 5
            if kill -0 $pid 2>/dev/null; then
                kill -KILL $pid
            fi
        fi
        rm -f "$pid_file"
    fi
    
    # Wait a bit before restarting
    sleep 10
    
    # Restart the process
    run_optimized_process $process_id $min_id $max_id
}

# Function to check if all processes are done
check_processes() {
    local all_done=true
    local running_count=0
    local stuck_count=0
    
    for i in $(seq 0 $((NUM_PROCESSES - 1))); do
        local status=$(check_process_health $i)
        case $status in
            0) # Running
                all_done=false
                running_count=$((running_count + 1))
                ;;
            1) # Stopped
                echo "✅ Process $i completed"
                ;;
            2) # Stuck
                all_done=false
                stuck_count=$((stuck_count + 1))
                echo "⚠️  Process $i is stuck, will restart"
                restart_process $i
                ;;
        esac
    done
    
    echo "Status: $running_count running, $stuck_count restarted"
    echo $all_done
}

# Function to show enhanced progress
show_enhanced_progress() {
    echo ""
    echo "=== 📊 Enhanced Progress Report ==="
    local total_processed=0
    local total_api_calls=0
    local total_cache_hits=0
    local total_failures=0
    
    for i in $(seq 0 $((NUM_PROCESSES - 1))); do
        if [ -f "checkpoint_${i}.json" ]; then
            local processed=$(python3 -c "
import json
try:
    with open('checkpoint_${i}.json', 'r') as f:
        data = json.load(f)
        print(len(data.get('completed_songs', [])))
except:
    print(0)
")
            echo "🎵 Process $i: $processed songs completed"
            total_processed=$((total_processed + processed))
        else
            echo "🎵 Process $i: No checkpoint found"
        fi
        
        # Show circuit breaker status and stats from logs
        if [ -f "optimized_process_${i}.log" ]; then
            local last_stats=$(tail -20 "optimized_process_${i}.log" | grep -o "API calls: [0-9]*" | tail -1 | grep -o "[0-9]*" || echo "0")
            local cache_hits=$(tail -20 "optimized_process_${i}.log" | grep -o "Cache hits: [0-9]*" | tail -1 | grep -o "[0-9]*" || echo "0")
            local success_rate=$(tail -20 "optimized_process_${i}.log" | grep -o "Success rate: [0-9.]*%" | tail -1 | grep -o "[0-9.]*" || echo "0")
            
            if [ "$last_stats" != "0" ]; then
                echo "   📈 API calls: $last_stats, Cache hits: $cache_hits, Success rate: ${success_rate}%"
                total_api_calls=$((total_api_calls + last_stats))
                total_cache_hits=$((total_cache_hits + cache_hits))
            fi
        fi
    done
    
    echo ""
    echo "🎯 Overall Statistics:"
    echo "   Total processed: $total_processed / $TOTAL_SONGS"
    local percentage=$((total_processed * 100 / TOTAL_SONGS))
    echo "   Overall progress: $percentage%"
    echo "   Total API calls: $total_api_calls"
    echo "   Total cache hits: $total_cache_hits"
    
    if [ $total_api_calls -gt 0 ]; then
        local cache_hit_rate=$((total_cache_hits * 100 / (total_api_calls + total_cache_hits)))
        echo "   Cache hit rate: ${cache_hit_rate}%"
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

# Function to show circuit breaker status
show_circuit_breaker_status() {
    echo "🔌 Circuit Breaker Status:"
    for i in $(seq 0 $((NUM_PROCESSES - 1))); do
        if [ -f "optimized_process_${i}.log" ]; then
            local cb_status=$(tail -50 "optimized_process_${i}.log" | grep -i "circuit breaker" | tail -1 || echo "")
            if [ -n "$cb_status" ]; then
                echo "   Process $i: $cb_status"
            else
                echo "   Process $i: Normal operation"
            fi
        fi
    done
    echo ""
}

# Function to cleanup
cleanup() {
    echo ""
    echo "=== 🧹 Cleanup ==="
    for i in $(seq 0 $((NUM_PROCESSES - 1))); do
        if [ -f "optimized_process_${i}.pid" ]; then
            local pid=$(cat "optimized_process_${i}.pid")
            if kill -0 $pid 2>/dev/null; then
                echo "🛑 Terminating process $i (PID: $pid)"
                kill -TERM $pid
                sleep 3
                if kill -0 $pid 2>/dev/null; then
                    kill -KILL $pid
                fi
            fi
            rm -f "optimized_process_${i}.pid"
        fi
    done
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Main execution
echo "🚀 Starting all optimized processes..."
echo ""

# Start all processes with staggered timing
for i in $(seq 0 $((NUM_PROCESSES - 1))); do
    range=(${RANGES[$i]})
    min_id=${range[0]}
    max_id=${range[1]}
    
    run_optimized_process $i $min_id $max_id
    sleep 15  # Longer delay between starts for better rate limit distribution
done

echo ""
echo "🎵 All processes started. Monitoring progress with enhanced features..."
echo "🔍 Features: Circuit breaker monitoring, stuck process detection, auto-restart"
echo "⏱️  Press Ctrl+C to stop all processes"
echo ""

# Enhanced monitoring loop
monitoring_cycle=0
while true; do
    monitoring_cycle=$((monitoring_cycle + 1))
    
    if [ "$(check_processes)" = "true" ]; then
        echo "🎉 All processes completed!"
        show_enhanced_progress
        break
    fi
    
    # Show different information on different cycles
    case $((monitoring_cycle % 3)) in
        1)
            show_enhanced_progress
            ;;
        2)
            show_circuit_breaker_status
            ;;
        0)
            echo "💓 Heartbeat check... (cycle $monitoring_cycle)"
            ;;
    esac
    
    sleep 30  # Check every 30 seconds
done

echo ""
echo "=== 🏁 Final Summary ==="
show_enhanced_progress

# Merge all cache files
echo "🔄 Merging cache files..."
python3 -c "
import pickle
import os
import json

merged_cache = {}
merged_failed = set()
total_checkpoints = {}

for i in range($NUM_PROCESSES):
    # Merge caches
    cache_file = f'artist_genre_cache_{i}.pkl'
    if os.path.exists(cache_file):
        with open(cache_file, 'rb') as f:
            cache = pickle.load(f)
            merged_cache.update(cache)
        print(f'✅ Merged cache from process {i}: {len(cache)} entries')
    
    # Merge failed songs
    failed_file = f'failed_songs_{i}.json'
    if os.path.exists(failed_file):
        with open(failed_file, 'r') as f:
            failed = set(json.load(f))
            merged_failed.update(failed)
        print(f'📋 Merged failed songs from process {i}: {len(failed)} entries')
    
    # Collect checkpoint info
    checkpoint_file = f'checkpoint_{i}.json'
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            checkpoint = json.load(f)
            total_checkpoints[i] = checkpoint

# Save merged cache
with open('artist_genre_cache_ultra_merged.pkl', 'wb') as f:
    pickle.dump(merged_cache, f)

# Save merged failed songs
with open('failed_songs_merged.json', 'w') as f:
    json.dump(list(merged_failed), f, indent=2)

# Save final summary
summary = {
    'total_cache_entries': len(merged_cache),
    'total_failed_songs': len(merged_failed),
    'process_checkpoints': total_checkpoints,
    'completion_time': '$(date)',
    'cache_hit_efficiency': len(merged_cache) / (len(merged_cache) + len(merged_failed)) * 100 if (len(merged_cache) + len(merged_failed)) > 0 else 0
}

with open('processing_summary.json', 'w') as f:
    json.dump(summary, f, indent=2)

print(f'')
print(f'🎯 Final merged cache: {len(merged_cache)} entries')
print(f'❌ Total failed songs: {len(merged_failed)} entries')
print(f'💾 Summary saved to processing_summary.json')
"

echo ""
echo "🎉 Ultra-optimized processing complete!"
echo "📁 Check processing_summary.json for detailed results"