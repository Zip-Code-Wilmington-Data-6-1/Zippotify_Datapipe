#!/usr/bin/env python3
"""
Live Monitoring for High-Speed Spotify Genre Processing
Real-time progress tracking with stats and ETA
"""

import os
import time
import json
import subprocess
from datetime import datetime, timedelta
import sys

def clear_screen():
    os.system('clear' if os.name == 'posix' else 'cls')

def get_process_status(process_id):
    """Check if process is running"""
    pid_file = f"speed_{process_id}.pid"
    if os.path.exists(pid_file):
        try:
            with open(pid_file, 'r') as f:
                pid = f.read().strip()
            # Check if process is still running
            result = subprocess.run(['ps', '-p', pid], capture_output=True, text=True)
            return result.returncode == 0
        except:
            return False
    return False

def get_progress_from_logs(process_id):
    """Extract latest progress from log files"""
    log_file = f"speed_{process_id}.log"
    if not os.path.exists(log_file):
        return 0, "No log file"
    
    try:
        # Get last few lines and look for progress
        result = subprocess.run(['tail', '-20', log_file], capture_output=True, text=True)
        lines = result.stdout.split('\n')
        
        latest_progress = 0
        status = "Initializing"
        
        for line in reversed(lines):
            if "Progress:" in line and "songs processed" in line:
                try:
                    # Extract number from "Progress: 1234 songs processed"
                    parts = line.split("Progress:")
                    if len(parts) > 1:
                        number_part = parts[1].split("songs")[0].strip()
                        latest_progress = int(number_part)
                        status = "Processing"
                        break
                except:
                    continue
            elif "completed successfully" in line.lower():
                status = "Completed"
                break
            elif "rate/request limit" in line:
                status = "Rate Limited"
            elif "Processing batch:" in line:
                status = "Processing"
        
        return latest_progress, status
    except Exception as e:
        return 0, f"Error: {e}"

def calculate_rate_and_eta(progress_data, start_time):
    """Calculate processing rate and ETA"""
    if not progress_data or len(progress_data) < 2:
        return 0, "Unknown"
    
    # Calculate rate from last few data points
    recent_data = progress_data[-10:]  # Last 10 readings
    
    if len(recent_data) < 2:
        return 0, "Unknown"
    
    time_diff = recent_data[-1][0] - recent_data[0][0]
    progress_diff = recent_data[-1][1] - recent_data[0][1]
    
    if time_diff <= 0:
        return 0, "Unknown"
    
    rate = progress_diff / time_diff * 3600  # songs per hour
    
    # Calculate ETA based on remaining songs
    total_processed = sum(data[1] for data in recent_data[-1:])
    remaining = 630000 - total_processed  # Approximate total
    
    if rate > 0:
        eta_hours = remaining / rate
        eta_time = datetime.now() + timedelta(hours=eta_hours)
        return rate, eta_time.strftime("%Y-%m-%d %H:%M")
    
    return rate, "Unknown"

def show_live_stats():
    """Main monitoring loop"""
    print("🚀 Starting Live Monitoring...")
    print("Press Ctrl+C to exit")
    time.sleep(2)
    
    # Check if start time file exists
    start_time_file = "speed_start_time.txt"
    if os.path.exists(start_time_file):
        with open(start_time_file, 'r') as f:
            start_timestamp = int(f.read().strip())
            start_time = datetime.fromtimestamp(start_timestamp)
    else:
        start_time = datetime.now()
    
    # Store progress history for rate calculation
    progress_history = []
    
    while True:
        try:
            clear_screen()
            
            print("🎵 HIGH-SPEED Spotify Genre Processing - LIVE MONITOR")
            print("=" * 70)
            print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            elapsed = datetime.now() - start_time
            print(f"Elapsed: {str(elapsed).split('.')[0]}")
            print(f"Updated: {datetime.now().strftime('%H:%M:%S')}")
            print("=" * 70)
            
            total_processed = 0
            active_processes = 0
            completed_processes = 0
            
            # Process status table
            print(f"{'Process':<8} {'Range':<20} {'Progress':<10} {'Status':<15} {'PID Status':<10}")
            print("-" * 70)
            
            ranges = [
                "1-105K", "105K-210K", "210K-315K", 
                "315K-420K", "420K-525K", "525K-700K"
            ]
            
            for i in range(6):
                progress, status = get_progress_from_logs(i)
                is_running = get_process_status(i)
                
                total_processed += progress
                
                if is_running:
                    pid_status = "🟢 Running"
                    active_processes += 1
                else:
                    if status == "Completed":
                        pid_status = "✅ Done"
                        completed_processes += 1
                    else:
                        pid_status = "🔴 Stopped"
                
                if status == "Rate Limited":
                    status = "⚠️ Rate Limit"
                elif status == "Processing":
                    status = "⚡ Processing"
                elif status == "Completed":
                    status = "✅ Complete"
                
                print(f"{i:<8} {ranges[i]:<20} {progress:<10,} {status:<15} {pid_status:<10}")
            
            print("-" * 70)
            
            # Overall statistics
            estimated_total = 630000  # Rough estimate
            percentage = (total_processed / estimated_total * 100) if estimated_total > 0 else 0
            
            print(f"📊 OVERALL STATISTICS:")
            print(f"   Total Processed: {total_processed:,} songs")
            print(f"   Overall Progress: {percentage:.1f}%")
            print(f"   Active Processes: {active_processes}/6")
            print(f"   Completed Processes: {completed_processes}/6")
            
            # Progress bar
            bar_width = 50
            filled_width = int(bar_width * percentage / 100)
            bar = "█" * filled_width + "░" * (bar_width - filled_width)
            print(f"   Progress: [{bar}] {percentage:.1f}%")
            
            # Store progress for rate calculation
            current_time = time.time()
            progress_history.append((current_time, total_processed))
            
            # Keep only last 30 minutes of data
            cutoff_time = current_time - 1800  # 30 minutes
            progress_history = [(t, p) for t, p in progress_history if t > cutoff_time]
            
            # Calculate rate and ETA
            if len(progress_history) >= 2:
                time_diff = progress_history[-1][0] - progress_history[0][0]
                progress_diff = progress_history[-1][1] - progress_history[0][1]
                
                if time_diff > 0:
                    rate = progress_diff / time_diff * 3600  # songs per hour
                    remaining = estimated_total - total_processed
                    
                    if rate > 0:
                        eta_hours = remaining / rate
                        eta_time = datetime.now() + timedelta(hours=eta_hours)
                        
                        print(f"   ⚡ Current Rate: {rate:.0f} songs/hour")
                        print(f"   ⏱️ ETA: {eta_time.strftime('%Y-%m-%d %H:%M')} ({eta_hours:.1f} hours)")
                        
                        # Success indicators
                        if eta_hours < 48:
                            print(f"   🎯 ON TARGET for 24-48 hour completion!")
                        elif eta_hours < 72:
                            print(f"   ✅ Good progress - under 3 days")
                        else:
                            print(f"   ⚠️ May need optimization")
            
            print()
            
            # Recent activity
            print("📝 RECENT ACTIVITY:")
            for i in range(6):
                log_file = f"speed_{i}.log"
                if os.path.exists(log_file):
                    try:
                        result = subprocess.run(['tail', '-1', log_file], capture_output=True, text=True)
                        last_line = result.stdout.strip()
                        if last_line and len(last_line) > 0:
                            # Extract timestamp and relevant info
                            if "Progress:" in last_line or "Processing batch:" in last_line or "Rate limit" in last_line:
                                # Show only time and key info
                                parts = last_line.split(' - ')
                                if len(parts) >= 3:
                                    time_part = parts[0].split(' ')[1] if ' ' in parts[0] else parts[0]
                                    info_part = parts[-1][:50] + "..." if len(parts[-1]) > 50 else parts[-1]
                                    print(f"   P{i} [{time_part}]: {info_part}")
                    except:
                        pass
            
            print()
            print("=" * 70)
            print("Press Ctrl+C to exit monitoring")
            
            # Wait 10 seconds before next update
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\n\n📊 Final Status:")
            print(f"   Total Processed: {total_processed:,} songs")
            print(f"   Active Processes: {active_processes}/6")
            print(f"   Runtime: {str(datetime.now() - start_time).split('.')[0]}")
            print("\n✅ Monitoring stopped. Processes continue running in background.")
            break
        except Exception as e:
            print(f"\n❌ Monitoring error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    show_live_stats()