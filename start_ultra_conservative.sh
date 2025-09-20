#!/bin/bash
# ULTRA CONSERVATIVE TEST
# 1 process, 1 worker, large delays

echo "🐌 ULTRA CONSERVATIVE TEST"
echo "   Target: 100-200 songs/hour"
echo "   Single process, single worker"
echo

# Store start time
echo $(date +%s) > ultra_start_time.txt

echo "Starting ultra-conservative process..."

nohup python spofify_genres.py \
    > ultra_conservative.log 2>&1 &

echo $! > ultra_conservative.pid

echo "✅ Ultra-conservative test started"
echo "📊 Monitor: tail -f ultra_conservative.log"
echo "⏰ Let run for 30 minutes to test"
