#!/usr/bin/env python3
"""
Phase Monitor - Track progress through scaling phases
"""

import os
import glob
import subprocess
from datetime import datetime

def get_current_phase():
    """Determine which phase is currently running"""
    phase_files = {
        1: glob.glob("phase1_*.pid"),
        2: glob.glob("phase2_*.pid"), 
        3: glob.glob("phase3_*.pid")
    }
    
    for phase, files in phase_files.items():
        if files:
            # Check if any processes are actually running
            running = 0
            for pid_file in files:
                try:
                    with open(pid_file, 'r') as f:
                        pid = f.read().strip()
                    result = subprocess.run(['ps', '-p', pid], capture_output=True)
                    if result.returncode == 0:
                        running += 1
                except:
                    pass
            if running > 0:
                return phase, running
    
    return None, 0

def get_phase_progress(phase):
    """Get progress for current phase"""
    log_pattern = f"phase{phase}_*.log"
    log_files = glob.glob(log_pattern)
    
    total_progress = 0
    for log_file in log_files:
        if os.path.exists(log_file):
            try:
                result = subprocess.run(['tail', '-10', log_file], capture_output=True, text=True)
                lines = result.stdout.split('\n')
                
                for line in reversed(lines):
                    if "Progress:" in line and "songs processed" in line:
                        try:
                            import re
                            match = re.search(r'Progress:\s*(\d+)\s*songs processed', line)
                            if match:
                                progress = int(match.group(1))
                                total_progress += progress
                                break
                        except:
                            continue
            except:
                pass
    
    return total_progress

def check_rate_limits(phase):
    """Check for rate limiting in current phase"""
    log_pattern = f"phase{phase}_*.log"
    log_files = glob.glob(log_pattern)
    
    rate_limits = 0
    for log_file in log_files:
        if os.path.exists(log_file):
            try:
                result = subprocess.run(['grep', '-c', 'rate/request limit', log_file], 
                                      capture_output=True, text=True)
                if result.stdout.strip().isdigit():
                    rate_limits += int(result.stdout.strip())
            except:
                pass
    
    return rate_limits

def main():
    print("🚀 PHASE SCALING MONITOR")
    print("=" * 50)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    current_phase, running_processes = get_current_phase()
    
    if current_phase:
        print(f"📍 CURRENT PHASE: {current_phase}")
        print(f"   Running processes: {running_processes}")
        
        progress = get_phase_progress(current_phase)
        print(f"   Progress: {progress:,} songs")
        
        rate_limits = check_rate_limits(current_phase)
        if rate_limits > 0:
            print(f"   ⚠️  Rate limits detected: {rate_limits}")
            print(f"   🚨 Consider stopping current phase")
        else:
            print(f"   ✅ No rate limits detected")
        
        # Phase-specific guidance
        if current_phase == 1:
            print()
            print("   PHASE 1 GUIDANCE:")
            print("   • Run for 2 hours minimum")
            print("   • If no rate limits: run ./start_phase2.sh")
            print("   • Target: 4,000 songs/hour")
            
        elif current_phase == 2:
            print()
            print("   PHASE 2 GUIDANCE:")
            print("   • Run for 6+ hours if stable")
            print("   • If no rate limits: run ./start_phase3.sh")
            print("   • Target: 8,000 songs/hour")
            
        elif current_phase == 3:
            print()
            print("   PHASE 3 GUIDANCE:")
            print("   • Maximum scale - monitor closely")
            print("   • Target: 12,000+ songs/hour")
            print("   • Should achieve 2-3 day completion")
    else:
        print("📍 NO ACTIVE PHASE DETECTED")
        print("   Start with: ./start_phase1.sh")
    
    print()
    print("=" * 50)

if __name__ == "__main__":
    main()
