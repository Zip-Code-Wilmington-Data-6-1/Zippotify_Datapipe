#!/usr/bin/env python3
"""
Intelligent Recovery Strategy for Spotify API Rate Limiting
Analysis of what went wrong and how to fix it
"""

import os
import re
import subprocess
from datetime import datetime, timedelta

def analyze_failure():
    """Analyze what caused the rate limiting"""
    print("🔍 FAILURE ANALYSIS:")
    print("-" * 50)
    
    # Check when rate limiting started
    try:
        result = subprocess.run(['grep', '-h', 'rate/request limit', 'speed_*.log'], 
                              capture_output=True, text=True)
        
        if result.stdout:
            lines = result.stdout.strip().split('\n')
            
            # Find first rate limit
            first_limit = None
            severe_limits = 0
            
            for line in lines:
                if '2025-09-20' in line and 'rate/request limit' in line:
                    if not first_limit:
                        timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                        if timestamp_match:
                            first_limit = timestamp_match.group(1)
                    
                    # Check for severe limits (>1 hour)
                    if ': ' in line:
                        try:
                            wait_time = int(line.split(': ')[-1])
                            if wait_time > 3600:
                                severe_limits += 1
                        except:
                            pass
            
            print(f"   First rate limit: {first_limit}")
            print(f"   Total rate limit events: {len(lines)}")
            print(f"   Severe limits (>1hr): {severe_limits}")
            
            # Calculate when it went wrong
            if first_limit:
                limit_time = datetime.strptime(first_limit, '%Y-%m-%d %H:%M:%S')
                print(f"   Duration of limits: {datetime.now() - limit_time}")
                
    except Exception as e:
        print(f"   Error analyzing: {e}")

def calculate_safe_parameters():
    """Calculate safe restart parameters"""
    print("\n🎯 SAFE RESTART PARAMETERS:")
    print("-" * 50)
    
    # Based on Spotify API limits:
    # - 100 requests per second per app
    # - But we were hitting limits, so actual limit is lower
    
    print("   CONSERVATIVE APPROACH:")
    print("   • Total concurrent requests: 8-12 (instead of 24)")
    print("   • Processes: 2-3 (instead of 6)")
    print("   • Workers per process: 2-3 (instead of 4)")
    print("   • Delay between batches: 5-10 seconds")
    print("   • Smaller batch sizes: 10-15 songs")
    print()
    print("   ESTIMATED NEW RATE:")
    print("   • Songs per hour: 2,000-4,000 (reduced but stable)")
    print("   • Completion time: 5-13 days (but no rate limit delays)")
    print("   • Better than current 95+ hour ETA with delays!")

def create_gentle_restart_script():
    """Create ultra-conservative restart script"""
    script_content = '''#!/bin/bash
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
    
    nohup python spotify_genres_optimized.py \\
        --process_id $process_id \\
        --min_id $min_id \\
        --max_id $max_id \\
        --max_workers 2 \\
        --batch_size 10 \\
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
'''
    
    with open('start_gentle.sh', 'w') as f:
        f.write(script_content)
    
    os.chmod('start_gentle.sh', 0o755)
    print("\n📝 Created start_gentle.sh - ultra-conservative restart")

def create_monitoring_update():
    """Update monitoring for gentle restart logs"""
    print("\n📊 MONITORING UPDATE NEEDED:")
    print("-" * 50)
    print("   Current monitors look for speed_*.log")
    print("   Gentle restart uses gentle_*.log")
    print("   Need to update monitoring scripts")
    print()
    print("   TEMPORARY MONITORING:")
    print("   tail -f gentle_*.log")
    print("   grep 'Progress:' gentle_*.log | tail -10")

def main():
    print("🚨 SPOTIFY API RECOVERY ANALYSIS")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    analyze_failure()
    calculate_safe_parameters()
    create_gentle_restart_script() 
    create_monitoring_update()
    
    print()
    print("🎯 RECOMMENDED RECOVERY PLAN:")
    print("=" * 60)
    print("1. ⏰ WAIT: 30-60 minutes minimum (API cooldown)")
    print("2. 🧪 TEST: ./start_gentle.sh (ultra-conservative)")
    print("3. 📊 MONITOR: Watch for any rate limit messages")
    print("4. 📈 SCALE: Only if no rate limits for 1+ hours")
    print()
    print("💡 KEY INSIGHT:")
    print("Better to process 3,000 songs/hour consistently")
    print("than get stuck with 95+ hour rate limit delays!")
    print("=" * 60)

if __name__ == "__main__":
    main()