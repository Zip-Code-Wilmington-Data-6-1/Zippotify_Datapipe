#!/bin/bash
# High-Speed Processing - Aggressive Settings for 1-2 Day Completion

set -e

# Configuration for speed
NUM_PROCESSES=8
BATCH_SIZE=100
MAX_WORKERS=4

# Aggressive ranges - 8 processes
RANGES=(
    "1 87500"
    "87501 175000"
    "175001 262500" 
    "262501 350000"
    "350001 437500"
    "437501 525000"
    "525001 612500"
    "612501 700000"
)

echo "🚀 HIGH-SPEED SPOTIFY PROCESSING"
echo "Target: Complete in 1-2 DAYS"
echo "Processes: $NUM_PROCESSES"
echo "Batch size: $BATCH_SIZE (larger batches = faster)"
echo "Workers per process: $MAX_WORKERS"
echo ""

# Function to run high-speed process
run_speed_process() {
    local process_id=$1
    local min_id=$2
    local max_id=$3
    
    echo "⚡ Starting Speed Process $process_id: IDs $min_id - $max_id"
    
    python spotify_genres_optimized.py \
        --min_id $min_id \
        --max_id $max_id \
        --process_id $process_id \
        --max_workers $MAX_WORKERS \
        --batch_size $BATCH_SIZE \
        > "speed_process_${process_id}.log" 2>&1 &
    
    echo $! > "speed_process_${process_id}.pid"
    echo "✅ Process $process_id started with PID: $(cat speed_process_${process_id}.pid)"
}

# Start all processes with minimal delay (aggressive)
for i in $(seq 0 $((NUM_PROCESSES - 1))); do
    range=(${RANGES[$i]})
    min_id=${range[0]}
    max_id=${range[1]}
    
    run_speed_process $i $min_id $max_id
    sleep 3  # Reduced delay for speed
done

echo ""
echo "🔥 All high-speed processes started!"
echo "📊 Expected completion: 24-48 hours"
echo "⚡ Processing ~800-1500 songs per hour total"
