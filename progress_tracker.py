#!/usr/bin/env python3
"""
Comprehensive Progress Tracker for Spotify Genre Processing
Handles multiple log file formats and provides detailed analytics
"""

import os
import re
import json
import subprocess
from datetime import datetime, timedelta
from collections import defaultdict
import glob

class ProgressTracker:
    def __init__(self):
        self.log_patterns = [
            "speed_*.log",
            "speed_process_*.log"
        ]
        self.total_target = 630000  # Estimated total songs
        self.process_ranges = {
            0: (1, 105000),
            1: (105001, 210000), 
            2: (210001, 315000),
            3: (315001, 420000),
            4: (420001, 525000),
            5: (525001, 700000)
        }
    
    def find_all_log_files(self):
        """Find all relevant log files"""
        log_files = []
        for pattern in self.log_patterns:
            log_files.extend(glob.glob(pattern))
        return sorted(list(set(log_files)))
    
    def extract_process_id(self, filename):
        """Extract process ID from filename"""
        # Handle speed_0.log, speed_process_0.log, etc.
        match = re.search(r'speed_(?:process_)?(\d+)\.log', filename)
        return int(match.group(1)) if match else None
    
    def get_latest_progress(self, log_file):
        """Get latest progress from a log file"""
        if not os.path.exists(log_file):
            return 0, "No file", None
        
        try:
            # Get last 20 lines to find progress
            result = subprocess.run(['tail', '-20', log_file], capture_output=True, text=True)
            lines = result.stdout.strip().split('\n')
            
            latest_progress = 0
            status = "Initializing"
            timestamp = None
            
            for line in reversed(lines):
                if "Progress:" in line and "songs processed" in line:
                    try:
                        # Extract progress number
                        progress_match = re.search(r'Progress:\s*(\d+)\s*songs processed', line)
                        if progress_match:
                            latest_progress = int(progress_match.group(1))
                            status = "Processing"
                            
                            # Extract timestamp
                            timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                            if timestamp_match:
                                timestamp = datetime.strptime(timestamp_match.group(1), '%Y-%m-%d %H:%M:%S')
                            break
                    except (ValueError, AttributeError):
                        continue
                elif "completed successfully" in line.lower():
                    status = "Completed"
                    break
                elif "rate limit" in line.lower():
                    status = "Rate Limited"
                elif "error" in line.lower():
                    status = "Error"
            
            return latest_progress, status, timestamp
            
        except Exception as e:
            return 0, f"Error reading file: {e}", None
    
    def get_process_status(self, process_id):
        """Check if process is still running"""
        pid_file = f"speed_{process_id}.pid"
        if os.path.exists(pid_file):
            try:
                with open(pid_file, 'r') as f:
                    pid = f.read().strip()
                result = subprocess.run(['ps', '-p', pid], capture_output=True, text=True)
                return result.returncode == 0
            except:
                return False
        return False
    
    def calculate_rates(self, log_file, minutes_back=30):
        """Calculate processing rate from log history"""
        if not os.path.exists(log_file):
            return 0, []
        
        try:
            # Get more lines for rate calculation
            result = subprocess.run(['tail', '-100', log_file], capture_output=True, text=True)
            lines = result.stdout.strip().split('\n')
            
            progress_points = []
            cutoff_time = datetime.now() - timedelta(minutes=minutes_back)
            
            for line in lines:
                if "Progress:" in line and "songs processed" in line:
                    try:
                        # Extract timestamp and progress
                        timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                        progress_match = re.search(r'Progress:\s*(\d+)\s*songs processed', line)
                        
                        if timestamp_match and progress_match:
                            timestamp = datetime.strptime(timestamp_match.group(1), '%Y-%m-%d %H:%M:%S')
                            progress = int(progress_match.group(1))
                            
                            if timestamp > cutoff_time:
                                progress_points.append((timestamp, progress))
                    except (ValueError, AttributeError):
                        continue
            
            # Calculate rate
            if len(progress_points) >= 2:
                progress_points.sort(key=lambda x: x[0])
                time_diff = (progress_points[-1][0] - progress_points[0][0]).total_seconds()
                progress_diff = progress_points[-1][1] - progress_points[0][1]
                
                if time_diff > 0:
                    rate = progress_diff / time_diff * 3600  # songs per hour
                    return rate, progress_points
            
            return 0, progress_points
            
        except Exception as e:
            return 0, []
    
    def get_database_progress(self):
        """Get progress directly from database if available"""
        try:
            # Try to get actual count from database
            result = subprocess.run([
                'python', '-c', '''
import sys
sys.path.append(".")
from database import get_db_connection
from models import DimSongGenre
from sqlalchemy import func

try:
    db = next(get_db_connection())
    count = db.query(func.count(DimSongGenre.song_id)).scalar()
    print(f"DATABASE_COUNT:{count}")
except Exception as e:
    print(f"DATABASE_ERROR:{e}")
                '''
            ], capture_output=True, text=True, cwd='.')
            
            output = result.stdout.strip()
            if "DATABASE_COUNT:" in output:
                count = int(output.split("DATABASE_COUNT:")[1])
                return count
        except:
            pass
        
        return None
    
    def generate_report(self):
        """Generate comprehensive progress report"""
        print("🎵 COMPREHENSIVE PROGRESS TRACKER")
        print("=" * 80)
        print(f"Report Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Find all log files
        log_files = self.find_all_log_files()
        print(f"📁 Found {len(log_files)} log files:")
        for lf in log_files:
            size = os.path.getsize(lf) if os.path.exists(lf) else 0
            print(f"   {lf} ({size:,} bytes)")
        print()
        
        # Process data
        process_data = {}
        total_processed = 0
        active_processes = 0
        total_rate = 0
        
        # Group files by process ID
        process_files = defaultdict(list)
        for log_file in log_files:
            process_id = self.extract_process_id(log_file)
            if process_id is not None:
                process_files[process_id].append(log_file)
        
        print("📊 PROCESS STATUS:")
        print(f"{'ID':<3} {'Range':<15} {'Progress':<10} {'Rate/hr':<10} {'Status':<12} {'PID':<8} {'Last Update'}")
        print("-" * 80)
        
        for process_id in sorted(process_files.keys()):
            files = process_files[process_id]
            
            # Find the file with most recent progress
            best_progress = 0
            best_status = "Unknown"
            best_timestamp = None
            best_rate = 0
            
            for log_file in files:
                progress, status, timestamp = self.get_latest_progress(log_file)
                rate, _ = self.calculate_rates(log_file)
                
                if progress > best_progress or (progress == best_progress and timestamp and 
                    (not best_timestamp or timestamp > best_timestamp)):
                    best_progress = progress
                    best_status = status
                    best_timestamp = timestamp
                    best_rate = rate
            
            # Check if process is running
            is_running = self.get_process_status(process_id)
            
            # Get range info
            range_start, range_end = self.process_ranges.get(process_id, (0, 0))
            range_str = f"{range_start//1000}K-{range_end//1000}K"
            
            pid_status = "🟢 Running" if is_running else ("🔴 Stopped" if best_progress > 0 else "⚪ Idle")
            
            last_update = best_timestamp.strftime("%H:%M:%S") if best_timestamp else "Unknown"
            
            print(f"{process_id:<3} {range_str:<15} {best_progress:<10,} {best_rate:<10.0f} {best_status:<12} {pid_status:<8} {last_update}")
            
            total_processed += best_progress
            if is_running:
                active_processes += 1
            total_rate += best_rate
        
        print("-" * 80)
        
        # Overall statistics
        percentage = (total_processed / self.total_target * 100) if self.total_target > 0 else 0
        
        print()
        print("📈 OVERALL STATISTICS:")
        print(f"   Total Processed: {total_processed:,} songs")
        print(f"   Target Total: {self.total_target:,} songs")
        print(f"   Progress: {percentage:.2f}%")
        print(f"   Remaining: {self.total_target - total_processed:,} songs")
        print(f"   Active Processes: {active_processes}/6")
        
        # Progress bar
        bar_width = 50
        filled_width = int(bar_width * percentage / 100)
        bar = "█" * filled_width + "░" * (bar_width - filled_width)
        print(f"   [{bar}] {percentage:.1f}%")
        
        # Rate and ETA calculations
        if total_rate > 0:
            remaining = self.total_target - total_processed
            eta_hours = remaining / total_rate
            eta_time = datetime.now() + timedelta(hours=eta_hours)
            
            print()
            print("⚡ PERFORMANCE METRICS:")
            print(f"   Combined Rate: {total_rate:.0f} songs/hour")
            print(f"   ETA: {eta_time.strftime('%Y-%m-%d %H:%M')} ({eta_hours:.1f} hours)")
            
            if eta_hours <= 48:
                print(f"   🎯 ON TARGET for 24-48 hour completion!")
            elif eta_hours <= 72:
                print(f"   ✅ Good progress - under 3 days")
            else:
                print(f"   ⚠️ May need optimization")
        
        # Database verification (if available)
        db_count = self.get_database_progress()
        if db_count is not None:
            print()
            print("💾 DATABASE VERIFICATION:")
            print(f"   Records in DB: {db_count:,}")
            print(f"   Log vs DB diff: {total_processed - db_count:,}")
        
        print()
        print("=" * 80)
        
        return {
            'total_processed': total_processed,
            'percentage': percentage,
            'active_processes': active_processes,
            'total_rate': total_rate,
            'eta_hours': eta_hours if 'eta_hours' in locals() else None
        }

def main():
    tracker = ProgressTracker()
    tracker.generate_report()

if __name__ == "__main__":
    main()