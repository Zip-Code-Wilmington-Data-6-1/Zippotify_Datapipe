#!/usr/bin/env python3
"""
Simple Live Dashboard - Quick Status Check
For when you just want a quick overview
"""

import os
import subprocess
from datetime import datetime

def get_quick_status():
    """Quick status check without clearing screen"""
    print(f"\n🎵 SPOTIFY PROCESSING - Quick Status [{datetime.now().strftime('%H:%M:%S')}]")
    print("=" * 60)
    
    total_processed = 0
    active_processes = 0
    
    ranges = ["1-105K", "105K-210K", "210K-315K", "315K-420K", "420K-525K", "525K-700K"]
    
    for i in range(6):
        # Check if process is running
        pid_file = f"speed_{i}.pid"
        is_running = False
        if os.path.exists(pid_file):
            try:
                with open(pid_file, 'r') as f:
                    pid = f.read().strip()
                result = subprocess.run(['ps', '-p', pid], capture_output=True, text=True)
                is_running = result.returncode == 0
            except:
                pass
        
        # Get progress from log
        progress = 0
        log_file = f"speed_{i}.log"
        if os.path.exists(log_file):
            try:
                result = subprocess.run(['tail', '-5', log_file], capture_output=True, text=True)
                lines = result.stdout.split('\n')
                for line in reversed(lines):
                    if "Progress:" in line and "songs processed" in line:
                        try:
                            parts = line.split("Progress:")
                            if len(parts) > 1:
                                progress = int(parts[1].split("songs")[0].strip())
                                break
                        except:
                            continue
            except:
                pass
        
        total_processed += progress
        if is_running:
            active_processes += 1
        
        status = "🟢 Running" if is_running else ("🔴 Stopped" if progress > 0 else "⚪ Not Started")
        print(f"P{i} {ranges[i]:<12}: {progress:>6,} songs {status}")
    
    percentage = (total_processed / 630000 * 100) if total_processed > 0 else 0
    print("=" * 60)
    print(f"Total: {total_processed:,} songs ({percentage:.1f}%) | Active: {active_processes}/6")
    
    if total_processed > 0:
        # Quick rate calculation from most recent logs
        try:
            result = subprocess.run(['grep', 'Progress:', 'speed_*.log'], capture_output=True, text=True)
            lines = result.stdout.strip().split('\n')[-10:]  # Last 10 progress entries
            if len(lines) >= 2:
                print(f"Recent activity detected - check logs for detailed rates")
        except:
            pass

if __name__ == "__main__":
    get_quick_status()