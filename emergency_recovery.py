#!/usr/bin/env python3
"""
EMERGENCY: Rate Limit Recovery Tool
Spotify API is severely rate limiting - need immediate action
"""

import os
import subprocess
import time
from datetime import datetime

def stop_all_processes():
    """Stop all running processes immediately"""
    print("🚨 EMERGENCY STOP: Stopping all processes due to severe rate limiting")
    
    stopped = 0
    for i in range(6):
        pid_file = f"speed_{i}.pid"
        if os.path.exists(pid_file):
            try:
                with open(pid_file, 'r') as f:
                    pid = f.read().strip()
                
                # Check if process is running
                result = subprocess.run(['ps', '-p', pid], capture_output=True)
                if result.returncode == 0:
                    print(f"   Stopping process {i} (PID: {pid})")
                    subprocess.run(['kill', pid])
                    stopped += 1
                    time.sleep(1)
                
                # Remove PID file
                os.remove(pid_file)
                
            except Exception as e:
                print(f"   Error stopping process {i}: {e}")
    
    print(f"✅ Stopped {stopped} processes")
    return stopped

def analyze_rate_limits():
    """Analyze current rate limiting situation"""
    print("\n📊 RATE LIMIT ANALYSIS:")
    
    # Check recent rate limit messages
    try:
        result = subprocess.run(['grep', '-h', 'rate/request limit', 'speed_*.log'], 
                              capture_output=True, text=True)
        
        if result.stdout:
            lines = result.stdout.strip().split('\n')
            recent_limits = [line for line in lines if '2025-09-20 11:' in line]
            
            print(f"   Recent rate limits: {len(recent_limits)} in last hour")
            
            # Extract wait times
            wait_times = []
            for line in recent_limits[-10:]:  # Last 10
                if ': ' in line:
                    try:
                        wait_time = int(line.split(': ')[-1])
                        wait_times.append(wait_time)
                    except:
                        pass
            
            if wait_times:
                avg_wait = sum(wait_times) / len(wait_times)
                max_wait = max(wait_times)
                print(f"   Average wait time: {avg_wait:.0f} seconds ({avg_wait/3600:.1f} hours)")
                print(f"   Maximum wait time: {max_wait:,} seconds ({max_wait/3600:.1f} hours)")
                
                if max_wait > 3600:
                    print("   🚨 CRITICAL: Wait times exceed 1 hour!")
                
    except Exception as e:
        print(f"   Error analyzing logs: {e}")

def recommend_recovery():
    """Provide recovery recommendations"""
    print("\n🎯 RECOVERY RECOMMENDATIONS:")
    print()
    print("IMMEDIATE ACTIONS:")
    print("1. ✅ All processes stopped to prevent further rate limiting")
    print("2. 🕐 Wait 15-30 minutes before any Spotify API calls")
    print("3. 🔧 Reduce concurrency when restarting")
    print()
    print("RESTART STRATEGY:")
    print("• Use only 2-3 processes instead of 6")
    print("• Reduce workers from 4 to 2 per process") 
    print("• Increase batch intervals (add delays)")
    print("• Start with 1 process to test API status")
    print()
    print("COMMANDS FOR GENTLE RESTART:")
    print("# Test with single process first:")
    print("python spotify_genres_optimized.py --process_id 0 --min_id 1 --max_id 10000 --max_workers 1 --batch_size 10")
    print()
    print("# If successful, use reduced load:")
    print("./start_reduced_load.sh  # (will create this script)")

def create_reduced_load_script():
    """Create a script for gentle restart"""
    script_content = '''#!/bin/bash
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
    
    nohup python spotify_genres_optimized.py \\
        --process_id $process_id \\
        --min_id $min_id \\
        --max_id $max_id \\
        --max_workers 2 \\
        --batch_size 15 \\
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
'''
    
    with open('start_reduced_load.sh', 'w') as f:
        f.write(script_content)
    
    os.chmod('start_reduced_load.sh', 0o755)
    print("📝 Created start_reduced_load.sh for gentle restart")

def main():
    print("🚨 SPOTIFY API RATE LIMIT EMERGENCY RECOVERY")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Stop all processes
    stopped = stop_all_processes()
    
    # Analyze the situation
    analyze_rate_limits()
    
    # Create recovery tools
    create_reduced_load_script()
    
    # Provide recommendations
    recommend_recovery()
    
    print()
    print("=" * 60)
    print("⏰ WAIT 15-30 MINUTES before attempting restart")
    print("🔧 Use reduced load strategy when ready")
    print("📊 Monitor API status before full restart")
    print("=" * 60)

if __name__ == "__main__":
    main()