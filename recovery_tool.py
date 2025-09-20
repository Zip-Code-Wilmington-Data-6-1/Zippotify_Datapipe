#!/usr/bin/env python3
"""
Emergency Recovery Tool for Spotify Genre Processing
Handles rate limit emergencies, stuck processes, and data recovery
"""

import os
import json
import pickle
import time
import signal
import psutil
from datetime import datetime, timedelta
import argparse
from pathlib import Path

class ProcessingRecovery:
    def __init__(self):
        self.process_files = []
        self.scan_for_files()
    
    def scan_for_files(self):
        """Scan for all processing-related files"""
        patterns = [
            "optimized_process_*.pid",
            "checkpoint_*.json", 
            "artist_genre_cache_*.pkl",
            "failed_songs_*.json",
            "progress_*.json"
        ]
        
        for pattern in patterns:
            files = list(Path(".").glob(pattern))
            self.process_files.extend(files)
        
        print(f"Found {len(self.process_files)} processing files")
    
    def emergency_stop_all(self):
        """Emergency stop all running processes"""
        print("🚨 EMERGENCY STOP - Terminating all processes")
        
        stopped_count = 0
        
        # Stop processes from PID files
        for pid_file in Path(".").glob("optimized_process_*.pid"):
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                
                if psutil.pid_exists(pid):
                    process = psutil.Process(pid)
                    print(f"🛑 Terminating process {pid_file.stem}: PID {pid}")
                    
                    # Graceful termination first
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except psutil.TimeoutExpired:
                        print(f"⚡ Force killing process {pid}")
                        process.kill()
                    
                    stopped_count += 1
                
                os.remove(pid_file)
                
            except Exception as e:
                print(f"❌ Error stopping process from {pid_file}: {e}")
        
        # Also look for any python processes running spotify scripts
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['name'] == 'python' and proc.info['cmdline']:
                    cmdline = ' '.join(proc.info['cmdline'])
                    if 'spotify_genres' in cmdline:
                        print(f"🎯 Found spotify process: PID {proc.info['pid']}")
                        proc = psutil.Process(proc.info['pid'])
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except psutil.TimeoutExpired:
                            proc.kill()
                        stopped_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        print(f"✅ Stopped {stopped_count} processes")
    
    def analyze_progress(self):
        """Analyze current progress from all checkpoints"""
        print("\n📊 Progress Analysis")
        print("=" * 50)
        
        total_completed = 0
        total_failed = 0
        process_info = {}
        
        for i in range(10):  # Check up to 10 processes
            checkpoint_file = f"checkpoint_{i}.json"
            if os.path.exists(checkpoint_file):
                try:
                    with open(checkpoint_file, 'r') as f:
                        checkpoint = json.load(f)
                    
                    completed = len(checkpoint.get('completed_songs', []))
                    failed = len(checkpoint.get('failed_songs', []))
                    current_id = checkpoint.get('song_id', 0)
                    timestamp = checkpoint.get('timestamp', '')
                    
                    total_completed += completed
                    total_failed += failed
                    
                    process_info[i] = {
                        'completed': completed,
                        'failed': failed,
                        'current_id': current_id,
                        'timestamp': timestamp
                    }
                    
                    print(f"Process {i}: {completed} completed, {failed} failed, at ID {current_id}")
                    
                except Exception as e:
                    print(f"❌ Error reading checkpoint {i}: {e}")
        
        print(f"\n🎯 Total: {total_completed} completed, {total_failed} failed")
        
        # Check for stale processes (no updates in 30+ minutes)
        now = datetime.now()
        stale_processes = []
        
        for proc_id, info in process_info.items():
            if info['timestamp']:
                try:
                    last_update = datetime.fromisoformat(info['timestamp'])
                    age = now - last_update
                    if age > timedelta(minutes=30):
                        stale_processes.append((proc_id, age))
                except:
                    pass
        
        if stale_processes:
            print(f"\n⚠️  Stale processes (no updates > 30min):")
            for proc_id, age in stale_processes:
                print(f"   Process {proc_id}: {age} old")
        
        return process_info
    
    def consolidate_caches(self):
        """Consolidate all cache files"""
        print("\n🔄 Consolidating Cache Files")
        print("=" * 30)
        
        merged_cache = {}
        cache_files = list(Path(".").glob("artist_genre_cache_*.pkl"))
        
        for cache_file in cache_files:
            try:
                with open(cache_file, 'rb') as f:
                    cache = pickle.load(f)
                
                print(f"📁 {cache_file}: {len(cache)} entries")
                merged_cache.update(cache)
                
            except Exception as e:
                print(f"❌ Error reading {cache_file}: {e}")
        
        if merged_cache:
            backup_file = f"cache_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
            with open(backup_file, 'wb') as f:
                pickle.dump(merged_cache, f)
            
            print(f"💾 Consolidated cache saved: {backup_file} ({len(merged_cache)} entries)")
        
        return merged_cache
    
    def consolidate_failed_songs(self):
        """Consolidate failed songs lists"""
        print("\n❌ Consolidating Failed Songs")
        print("=" * 30)
        
        all_failed = set()
        failed_files = list(Path(".").glob("failed_songs_*.json"))
        
        for failed_file in failed_files:
            try:
                with open(failed_file, 'r') as f:
                    failed = set(json.load(f))
                
                print(f"📁 {failed_file}: {len(failed)} entries")
                all_failed.update(failed)
                
            except Exception as e:
                print(f"❌ Error reading {failed_file}: {e}")
        
        if all_failed:
            backup_file = f"failed_songs_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(backup_file, 'w') as f:
                json.dump(list(all_failed), f, indent=2)
            
            print(f"💾 Consolidated failed songs saved: {backup_file} ({len(all_failed)} entries)")
        
        return all_failed
    
    def check_rate_limit_status(self):
        """Check recent rate limit issues from logs"""
        print("\n🚦 Rate Limit Status Check")
        print("=" * 30)
        
        log_files = list(Path(".").glob("*spotify*.log"))
        recent_rate_limits = []
        circuit_breaker_issues = []
        
        for log_file in log_files:
            try:
                with open(log_file, 'r') as f:
                    lines = f.readlines()
                
                # Check last 50 lines for recent issues
                recent_lines = lines[-50:] if len(lines) > 50 else lines
                
                for line in recent_lines:
                    if 'rate limited' in line.lower():
                        recent_rate_limits.append((log_file.name, line.strip()))
                    elif 'circuit breaker' in line.lower():
                        circuit_breaker_issues.append((log_file.name, line.strip()))
                        
            except Exception as e:
                print(f"❌ Error reading {log_file}: {e}")
        
        if recent_rate_limits:
            print("⚠️  Recent Rate Limits:")
            for log_file, line in recent_rate_limits[-5:]:  # Show last 5
                print(f"   {log_file}: {line}")
        else:
            print("✅ No recent rate limit issues found")
        
        if circuit_breaker_issues:
            print("\n🔌 Circuit Breaker Issues:")
            for log_file, line in circuit_breaker_issues[-3:]:  # Show last 3
                print(f"   {log_file}: {line}")
        else:
            print("✅ No circuit breaker issues found")
    
    def create_recovery_plan(self, process_info):
        """Create a recovery plan based on current state"""
        print("\n🛠️  Recovery Plan")
        print("=" * 20)
        
        # Determine ranges that need to be restarted
        ranges = [
            (1, 233333),
            (233334, 466666), 
            (466667, 700000)
        ]
        
        recovery_commands = []
        
        for i, (min_id, max_id) in enumerate(ranges):
            if i in process_info:
                current_id = process_info[i]['current_id']
                completed = process_info[i]['completed']
                
                if current_id < max_id:
                    # Process needs to continue
                    recovery_commands.append(
                        f"python spotify_genres_ultra_optimized.py "
                        f"--min_id {current_id} --max_id {max_id} "
                        f"--process_id {i} --max_workers 1 --batch_size 15"
                    )
                    print(f"📋 Process {i}: Resume from ID {current_id} ({completed} already done)")
                else:
                    print(f"✅ Process {i}: Complete")
            else:
                # Process never started or lost
                recovery_commands.append(
                    f"python spotify_genres_ultra_optimized.py "
                    f"--min_id {min_id} --max_id {max_id} "
                    f"--process_id {i} --max_workers 1 --batch_size 15"
                )
                print(f"🔄 Process {i}: Start fresh from ID {min_id}")
        
        if recovery_commands:
            print(f"\n📝 Recovery Commands:")
            for cmd in recovery_commands:
                print(f"   {cmd}")
            
            # Save recovery script
            recovery_script = "recovery_restart.sh"
            with open(recovery_script, 'w') as f:
                f.write("#!/bin/bash\n\n")
                f.write("# Auto-generated recovery script\n")
                f.write(f"# Generated: {datetime.now()}\n\n")
                
                for i, cmd in enumerate(recovery_commands):
                    f.write(f"# Process {i}\n")
                    f.write(f"{cmd} > recovery_process_{i}.log 2>&1 &\n")
                    f.write(f"echo $! > recovery_process_{i}.pid\n")
                    f.write("sleep 20  # Stagger starts\n\n")
                
                f.write("echo 'Recovery processes started'\n")
            
            os.chmod(recovery_script, 0o755)
            print(f"💾 Recovery script saved: {recovery_script}")
        else:
            print("🎉 No recovery needed - all processes complete!")

def main():
    parser = argparse.ArgumentParser(description="Emergency recovery tool")
    parser.add_argument("--stop", action="store_true", help="Emergency stop all processes")
    parser.add_argument("--analyze", action="store_true", help="Analyze current progress")
    parser.add_argument("--consolidate", action="store_true", help="Consolidate cache and failed files")
    parser.add_argument("--recovery", action="store_true", help="Create recovery plan")
    parser.add_argument("--full", action="store_true", help="Full analysis and recovery")
    
    args = parser.parse_args()
    
    recovery = ProcessingRecovery()
    
    if args.stop or args.full:
        recovery.emergency_stop_all()
        time.sleep(2)
    
    if args.analyze or args.full:
        process_info = recovery.analyze_progress()
        recovery.check_rate_limit_status()
    
    if args.consolidate or args.full:
        recovery.consolidate_caches()
        recovery.consolidate_failed_songs()
    
    if args.recovery or args.full:
        if 'process_info' in locals():
            recovery.create_recovery_plan(process_info)
        else:
            process_info = recovery.analyze_progress()
            recovery.create_recovery_plan(process_info)
    
    if not any([args.stop, args.analyze, args.consolidate, args.recovery, args.full]):
        print("🔧 Emergency Recovery Tool")
        print("Usage:")
        print("  python recovery_tool.py --stop       # Emergency stop all")
        print("  python recovery_tool.py --analyze    # Analyze progress")
        print("  python recovery_tool.py --consolidate # Merge cache files")
        print("  python recovery_tool.py --recovery   # Create recovery plan")
        print("  python recovery_tool.py --full       # Do everything")

if __name__ == "__main__":
    main()