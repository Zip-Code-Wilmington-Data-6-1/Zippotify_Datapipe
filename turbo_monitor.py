#!/usr/bin/env python3
"""
Turbo Monitor - High frequency monitoring for speed optimization
"""
import os
import json
import time
from datetime import datetime

def show_speed_stats():
    total_processed = 0
    processes_running = 0
    
    print(f"⚡ TURBO SPEED MONITOR - {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 60)
    
    for i in range(8):  # 8 processes
        progress_file = f"progress_{i}.json"
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r') as f:
                    data = json.load(f)
                    processed = data.get('total_processed', 0)
                    total_processed += processed
                    print(f"Process {i}: {processed:,} songs")
            except:
                pass
        
        # Check if process is running
        pid_file = f"speed_process_{i}.pid"
        if os.path.exists(pid_file):
            try:
                with open(pid_file, 'r') as f:
                    pid = f.read().strip()
                os.kill(int(pid), 0)  # Check if process exists
                processes_running += 1
            except:
                pass
    
    print(f"Total Processed: {total_processed:,}")
    print(f"Running Processes: {processes_running}/8")
    
    # Calculate speed
    if total_processed > 0:
        # Estimate based on time (rough)
        rate = total_processed  # This is a simple estimate
        estimated_completion_hours = (422605 - total_processed) / max(rate, 1) if rate > 0 else 0
        print(f"Estimated completion: {estimated_completion_hours:.1f} hours")
    
    print("=" * 60)

if __name__ == "__main__":
    while True:
        show_speed_stats()
        time.sleep(10)  # Update every 10 seconds for speed monitoring
