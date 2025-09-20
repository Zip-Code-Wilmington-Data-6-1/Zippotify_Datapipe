#!/usr/bin/env python3
"""
Smart Scaling Strategy for 2-3 Day Completion
Aggressive but intelligent approach to avoid rate limiting
"""

import os
import subprocess
from datetime import datetime, timedelta

def calculate_target_rates():
    """Calculate what rates we need for 2-3 day completion"""
    print("🎯 TARGET CALCULATION FOR 2-3 DAY COMPLETION:")
    print("=" * 60)
    
    total_songs = 630000
    processed_already = 8680  # From last status
    remaining = total_songs - processed_already
    
    print(f"   Total songs: {total_songs:,}")
    print(f"   Already processed: {processed_already:,}")
    print(f"   Remaining: {remaining:,}")
    print()
    
    # Calculate required rates
    for days in [2, 3]:
        hours = days * 24
        required_rate = remaining / hours
        print(f"   For {days}-day completion:")
        print(f"   • Required rate: {required_rate:,.0f} songs/hour")
        print(f"   • Per minute: {required_rate/60:.0f} songs/min")
        print(f"   • Completion: {(datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d %H:%M')}")
        print()
    
    return remaining

def analyze_rate_limit_patterns():
    """Analyze what caused rate limiting to find safe maximums"""
    print("🔍 RATE LIMIT PATTERN ANALYSIS:")
    print("=" * 60)
    
    print("   WHAT WE KNOW:")
    print("   • 6 processes × 4 workers = 24 concurrent → FAILED")
    print("   • Started getting rate limited around 11:21")
    print("   • Initial rate was ~17,000 songs/hour")
    print("   • Rate dropped to 6,000 before we stopped")
    print()
    
    print("   SAFE ZONE ANALYSIS:")
    print("   • Working rate: ~17,000 songs/hour initially")
    print("   • Breakdown point: ~24 concurrent requests")
    print("   • Safe concurrent: probably 8-15 requests")
    print("   • Sweet spot estimate: 12-16 concurrent requests")
    print()
    
    return 15  # Conservative safe concurrent limit

def create_smart_scaling_strategy(remaining_songs, safe_concurrent):
    """Create a smart scaling strategy"""
    print("🚀 SMART SCALING STRATEGY:")
    print("=" * 60)
    
    # Strategy: Start conservative, then scale up if no rate limits
    strategies = [
        {
            "name": "Phase 1: Conservative Test",
            "processes": 2,
            "workers": 3,
            "concurrent": 6,
            "batch_size": 15,
            "expected_rate": 4000,
            "duration_hours": 2,
            "test_phase": True
        },
        {
            "name": "Phase 2: Moderate Scale",
            "processes": 3,
            "workers": 4,
            "concurrent": 12,
            "batch_size": 20,
            "expected_rate": 8000,
            "duration_hours": 6,
            "test_phase": False
        },
        {
            "name": "Phase 3: Target Scale",
            "processes": 4,
            "workers": 4,
            "concurrent": 16,
            "batch_size": 25,
            "expected_rate": 12000,
            "duration_hours": 999,
            "test_phase": False
        }
    ]
    
    cumulative_hours = 0
    cumulative_processed = 0
    
    print("   PHASED SCALING PLAN:")
    print()
    
    for i, phase in enumerate(strategies, 1):
        if phase["test_phase"]:
            print(f"   {phase['name']}:")
            print(f"   • Processes: {phase['processes']} × {phase['workers']} workers = {phase['concurrent']} concurrent")
            print(f"   • Expected rate: {phase['expected_rate']:,} songs/hour")
            print(f"   • Duration: {phase['duration_hours']} hours (testing)")
            print(f"   • Purpose: Verify no rate limits")
            print()
        else:
            remaining_after_previous = remaining_songs - cumulative_processed
            hours_needed = remaining_after_previous / phase["expected_rate"]
            
            if hours_needed > phase["duration_hours"]:
                songs_in_phase = phase["expected_rate"] * phase["duration_hours"]
                hours_in_phase = phase["duration_hours"]
            else:
                songs_in_phase = remaining_after_previous
                hours_in_phase = hours_needed
            
            cumulative_processed += songs_in_phase
            cumulative_hours += hours_in_phase
            
            print(f"   {phase['name']}:")
            print(f"   • Processes: {phase['processes']} × {phase['workers']} workers = {phase['concurrent']} concurrent")
            print(f"   • Expected rate: {phase['expected_rate']:,} songs/hour")
            print(f"   • Songs in phase: {songs_in_phase:,.0f}")
            print(f"   • Phase duration: {hours_in_phase:.1f} hours")
            print(f"   • Cumulative: {cumulative_processed:,.0f} songs in {cumulative_hours:.1f} hours")
            print()
            
            if cumulative_processed >= remaining_songs * 0.99:  # 99% complete
                break
    
    total_completion_days = cumulative_hours / 24
    print(f"   🎯 ESTIMATED TOTAL COMPLETION: {total_completion_days:.1f} days")
    
    if total_completion_days <= 3:
        print("   ✅ TARGET ACHIEVABLE with smart scaling!")
    else:
        print("   ⚠️ May need Phase 4 with higher concurrency")
    
    return strategies

def create_phase_scripts(strategies):
    """Create scripts for each phase"""
    print("\n📝 CREATING PHASE SCRIPTS:")
    print("=" * 60)
    
    # Phase 1: Conservative test
    phase1_script = '''#!/bin/bash
# PHASE 1: Conservative Test (2 hours)
# Test if we can avoid rate limits with moderate load

echo "🧪 PHASE 1: Conservative Test"
echo "   Target: 4,000 songs/hour for 2 hours"
echo "   If successful → proceed to Phase 2"
echo

# Store start time for phase tracking
echo $(date +%s) > phase1_start_time.txt

start_phase1_process() {
    local process_id=$1
    local min_id=$2
    local max_id=$3
    
    echo "Starting Phase 1 Process $process_id: range $min_id-$max_id"
    
    nohup python spotify_genres_optimized.py \\
        --process_id $process_id \\
        --min_id $min_id \\
        --max_id $max_id \\
        --max_workers 3 \\
        --batch_size 15 \\
        > phase1_${process_id}.log 2>&1 &
    
    echo $! > phase1_${process_id}.pid
    sleep 60  # 1 minute between starts
}

# Start 2 processes with conservative settings
start_phase1_process 0 1 315000
start_phase1_process 1 315001 630000

echo "✅ Phase 1 started - 2 processes with 3 workers each"
echo "📊 Monitor: tail -f phase1_*.log"
echo "⏰ Run for 2 hours, then check for rate limits"
echo "🚦 If no rate limits: ./start_phase2.sh"
'''

    phase2_script = '''#!/bin/bash
# PHASE 2: Moderate Scale
# Scale up if Phase 1 was successful

echo "🚀 PHASE 2: Moderate Scale"
echo "   Target: 8,000 songs/hour"
echo "   3 processes × 4 workers = 12 concurrent"
echo

# Check Phase 1 results first
if grep -q "rate/request limit" phase1_*.log 2>/dev/null; then
    echo "❌ Phase 1 had rate limits - cannot proceed to Phase 2"
    echo "   Recommend staying with Phase 1 settings"
    exit 1
fi

echo "✅ Phase 1 clean - proceeding with Phase 2"

# Stop Phase 1 processes
for pid_file in phase1_*.pid; do
    if [ -f "$pid_file" ]; then
        kill $(cat "$pid_file") 2>/dev/null
        rm "$pid_file"
    fi
done

sleep 30

echo $(date +%s) > phase2_start_time.txt

start_phase2_process() {
    local process_id=$1
    local min_id=$2
    local max_id=$3
    
    echo "Starting Phase 2 Process $process_id: range $min_id-$max_id"
    
    nohup python spotify_genres_optimized.py \\
        --process_id $process_id \\
        --min_id $min_id \\
        --max_id $max_id \\
        --max_workers 4 \\
        --batch_size 20 \\
        > phase2_${process_id}.log 2>&1 &
    
    echo $! > phase2_${process_id}.pid
    sleep 45  # 45 seconds between starts  
}

# Start 3 processes
start_phase2_process 0 1 210000
start_phase2_process 1 210001 420000
start_phase2_process 2 420001 630000

echo "✅ Phase 2 started - 3 processes with 4 workers each"
echo "📊 Monitor: tail -f phase2_*.log"
'''

    phase3_script = '''#!/bin/bash
# PHASE 3: Target Scale
# Maximum safe scale for fastest completion

echo "🎯 PHASE 3: Target Scale"
echo "   Target: 12,000+ songs/hour"
echo "   4 processes × 4 workers = 16 concurrent"
echo

# Check Phase 2 results
if grep -q "rate/request limit" phase2_*.log 2>/dev/null; then
    echo "❌ Phase 2 had rate limits - cannot proceed to Phase 3"
    echo "   Staying with Phase 2 settings"
    exit 1
fi

echo "✅ Phase 2 clean - proceeding with Phase 3 (MAXIMUM SCALE)"

# Stop Phase 2 processes  
for pid_file in phase2_*.pid; do
    if [ -f "$pid_file" ]; then
        kill $(cat "$pid_file") 2>/dev/null
        rm "$pid_file"
    fi
done

sleep 45

echo $(date +%s) > phase3_start_time.txt

start_phase3_process() {
    local process_id=$1
    local min_id=$2
    local max_id=$3
    
    echo "Starting Phase 3 Process $process_id: range $min_id-$max_id"
    
    nohup python spotify_genres_optimized.py \\
        --process_id $process_id \\
        --min_id $min_id \\
        --max_id $max_id \\
        --max_workers 4 \\
        --batch_size 25 \\
        > phase3_${process_id}.log 2>&1 &
    
    echo $! > phase3_${process_id}.pid
    sleep 30  # 30 seconds between starts
}

# Start 4 processes for maximum throughput
start_phase3_process 0 1 157500
start_phase3_process 1 157501 315000  
start_phase3_process 2 315001 472500
start_phase3_process 3 472501 630000

echo "✅ Phase 3 started - 4 processes with 4 workers each"
echo "📊 Monitor: tail -f phase3_*.log"
echo "🎯 This should achieve 2-3 day completion target!"
'''

    # Write scripts
    scripts = [
        ('start_phase1.sh', phase1_script),
        ('start_phase2.sh', phase2_script), 
        ('start_phase3.sh', phase3_script)
    ]
    
    for filename, content in scripts:
        with open(filename, 'w') as f:
            f.write(content)
        os.chmod(filename, 0o755)
        print(f"   ✅ Created {filename}")

def create_phase_monitor():
    """Create monitoring script for phased approach"""
    monitor_script = '''#!/usr/bin/env python3
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
                lines = result.stdout.split('\\n')
                
                for line in reversed(lines):
                    if "Progress:" in line and "songs processed" in line:
                        try:
                            import re
                            match = re.search(r'Progress:\\s*(\\d+)\\s*songs processed', line)
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
'''
    
    with open('phase_monitor.py', 'w') as f:
        f.write(monitor_script)
    os.chmod('phase_monitor.py', 0o755)
    print(f"   ✅ Created phase_monitor.py")

def main():
    print("🎯 SMART SCALING FOR 2-3 DAY COMPLETION")
    print("=" * 70)
    print(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Calculate requirements
    remaining = calculate_target_rates()
    
    # Analyze safe limits
    safe_concurrent = analyze_rate_limit_patterns()
    
    # Create scaling strategy
    strategies = create_smart_scaling_strategy(remaining, safe_concurrent)
    
    # Create implementation scripts
    create_phase_scripts(strategies)
    create_phase_monitor()
    
    print()
    print("🚀 IMPLEMENTATION PLAN:")
    print("=" * 70)
    print("1. ⏰ WAIT 30-60 minutes (API cooldown)")
    print("2. 🧪 START: ./start_phase1.sh (conservative test)")
    print("3. 📊 MONITOR: python phase_monitor.py")
    print("4. 📈 SCALE: If no rate limits → ./start_phase2.sh")
    print("5. 🎯 TARGET: If clean → ./start_phase3.sh")
    print()
    print("💡 KEY SUCCESS FACTORS:")
    print("• Gradual scaling prevents rate limit catastrophe")
    print("• Continuous monitoring for early warning")
    print("• Fallback to previous phase if rate limited")
    print("• Should achieve 2-3 day target with Phase 3")
    print("=" * 70)

if __name__ == "__main__":
    main()