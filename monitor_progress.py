#!/usr/bin/env python3
"""
Spotify Genre Processing Monitor
Real-time monitoring of genre processing progress
"""

import json
import os
import time
import sys
from datetime import datetime, timedelta
import argparse

def load_progress(process_id):
    """Load progress for a specific process"""
    progress_file = f"progress_{process_id}.json"
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading progress for process {process_id}: {e}")
    return None

def load_cache_size(process_id):
    """Get cache file size"""
    cache_file = f"artist_genre_cache_{process_id}.pkl"
    if os.path.exists(cache_file):
        return os.path.getsize(cache_file)
    return 0

def format_time_elapsed(start_time_str):
    """Format elapsed time"""
    try:
        start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
        elapsed = datetime.now() - start_time.replace(tzinfo=None)
        return str(elapsed).split('.')[0]  # Remove microseconds
    except:
        return "Unknown"

def estimate_completion(processed, total, start_time_str):
    """Estimate completion time"""
    if processed == 0:
        return "Unknown"
    
    try:
        start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
        elapsed = datetime.now() - start_time.replace(tzinfo=None)
        rate = processed / elapsed.total_seconds()  # songs per second
        remaining = total - processed
        eta_seconds = remaining / rate if rate > 0 else 0
        eta = datetime.now() + timedelta(seconds=eta_seconds)
        return eta.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return "Unknown"

def format_bytes(bytes_size):
    """Format bytes in human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} TB"

def print_separator():
    """Print a separator line"""
    print("=" * 80)

def monitor_progress(num_processes=4, total_songs=630000, refresh_interval=10):
    """Monitor progress of all processes"""
    
    print("🎵 Spotify Genre Processing Monitor 🎵")
    print_separator()
    print(f"Monitoring {num_processes} processes")
    print(f"Total songs to process: {total_songs:,}")
    print(f"Refresh interval: {refresh_interval} seconds")
    print("\nPress Ctrl+C to exit monitor")
    print_separator()
    
    try:
        while True:
            os.system('clear' if os.name == 'posix' else 'cls')  # Clear screen
            
            print(f"🎵 Spotify Genre Processing Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print_separator()
            
            total_processed = 0
            active_processes = 0
            all_progress = []
            
            # Collect data from all processes
            for i in range(num_processes):
                progress = load_progress(i)
                cache_size = load_cache_size(i)
                
                if progress:
                    processed = progress.get('total_processed', 0)
                    current_id = progress.get('current_id', 0)
                    timestamp = progress.get('timestamp', '')
                    cache_entries = progress.get('cache_size', 0)
                    
                    total_processed += processed
                    active_processes += 1
                    
                    all_progress.append({
                        'process_id': i,
                        'processed': processed,
                        'current_id': current_id,
                        'timestamp': timestamp,
                        'cache_entries': cache_entries,
                        'cache_size': cache_size
                    })
                else:
                    all_progress.append({
                        'process_id': i,
                        'processed': 0,
                        'current_id': 0,
                        'timestamp': '',
                        'cache_entries': 0,
                        'cache_size': cache_size
                    })
            
            # Display individual process status
            print(f"{'Process':<8} {'Processed':<10} {'Current ID':<12} {'Cache Entries':<14} {'Cache Size':<12} {'Status':<15}")
            print("-" * 80)
            
            for p in all_progress:
                pid = p['process_id']
                processed = p['processed']
                current_id = p['current_id']
                cache_entries = p['cache_entries']
                cache_size = format_bytes(p['cache_size'])
                
                # Check if process is still running
                pid_file = f"process_{pid}.pid"
                if os.path.exists(pid_file):
                    try:
                        with open(pid_file, 'r') as f:
                            process_pid = f.read().strip()
                        # Check if process is still alive
                        os.kill(int(process_pid), 0)
                        status = "🟢 Running"
                    except (ProcessLookupError, ValueError):
                        status = "🔴 Stopped"
                        if os.path.exists(pid_file):
                            os.remove(pid_file)
                    except PermissionError:
                        status = "🟢 Running"
                else:
                    status = "⚪ Not Started" if processed == 0 else "🟡 Completed"
                
                print(f"{pid:<8} {processed:<10,} {current_id:<12,} {cache_entries:<14,} {cache_size:<12} {status:<15}")
            
            print_separator()
            
            # Overall statistics
            overall_percentage = (total_processed / total_songs * 100) if total_songs > 0 else 0
            
            print(f"📊 Overall Progress:")
            print(f"   Total Processed: {total_processed:,} / {total_songs:,} ({overall_percentage:.1f}%)")
            print(f"   Active Processes: {active_processes} / {num_processes}")
            
            # Progress bar
            bar_width = 50
            filled_width = int(bar_width * overall_percentage / 100)
            bar = "█" * filled_width + "░" * (bar_width - filled_width)
            print(f"   Progress: [{bar}] {overall_percentage:.1f}%")
            
            # Time estimates (using the first active process for estimation)
            active_progress = [p for p in all_progress if p['processed'] > 0 and p['timestamp']]
            if active_progress:
                sample = active_progress[0]
                elapsed = format_time_elapsed(sample['timestamp'])
                eta = estimate_completion(total_processed, total_songs, sample['timestamp'])
                print(f"   Time Elapsed: {elapsed}")
                print(f"   Estimated Completion: {eta}")
            
            print_separator()
            
            # Recent activity
            print("📝 Recent Activity:")
            log_files = [f"process_{i}.log" for i in range(num_processes)]
            recent_lines = []
            
            for log_file in log_files:
                if os.path.exists(log_file):
                    try:
                        with open(log_file, 'r') as f:
                            lines = f.readlines()
                            if lines:
                                recent_lines.append(f"[{log_file}] {lines[-1].strip()}")
                    except:
                        pass
            
            for line in recent_lines[-5:]:  # Show last 5 log entries
                print(f"   {line}")
            
            if not recent_lines:
                print("   No recent activity")
            
            print_separator()
            print(f"Next refresh in {refresh_interval} seconds... (Ctrl+C to exit)")
            
            time.sleep(refresh_interval)
            
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user.")
        print_separator()
        print("📋 Final Summary:")
        print(f"   Total Processed: {total_processed:,} / {total_songs:,} ({overall_percentage:.1f}%)")
        print(f"   Active Processes: {active_processes} / {num_processes}")

def main():
    parser = argparse.ArgumentParser(description="Monitor Spotify genre processing")
    parser.add_argument("--processes", type=int, default=4, help="Number of processes to monitor")
    parser.add_argument("--total", type=int, default=630000, help="Total number of songs")
    parser.add_argument("--interval", type=int, default=10, help="Refresh interval in seconds")
    
    args = parser.parse_args()
    
    monitor_progress(args.processes, args.total, args.interval)

if __name__ == "__main__":
    main()