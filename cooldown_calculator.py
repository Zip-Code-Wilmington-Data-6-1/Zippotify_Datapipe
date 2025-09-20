#!/usr/bin/env python3
"""
API Cooldown Calculator
Determines when it's safe to restart based on rate limiting history
"""

import re
from datetime import datetime, timedelta

def analyze_rate_limit_timeline():
    """Analyze when rate limiting started and escalated"""
    print("🕐 API COOLDOWN ANALYSIS")
    print("=" * 50)
    
    try:
        # Read all rate limit messages
        import subprocess
        result = subprocess.run(['grep', '-h', 'rate/request limit', 'speed_*.log'], 
                              capture_output=True, text=True)
        
        if not result.stdout:
            print("❌ No rate limit logs found")
            return
        
        lines = result.stdout.strip().split('\n')
        
        # Parse timestamps and wait times
        events = []
        for line in lines:
            timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
            wait_match = re.search(r'after:\s*(\d+)', line)
            
            if timestamp_match and wait_match:
                timestamp = datetime.strptime(timestamp_match.group(1), '%Y-%m-%d %H:%M:%S')
                wait_time = int(wait_match.group(1))
                events.append((timestamp, wait_time))
        
        if not events:
            print("❌ Could not parse rate limit events")
            return
        
        # Sort by timestamp
        events.sort(key=lambda x: x[0])
        
        print(f"📊 Rate Limit Timeline ({len(events)} events):")
        print()
        
        # Key events
        first_limit = events[0]
        severe_limits = [(ts, wait) for ts, wait in events if wait > 3600]  # > 1 hour
        last_severe = severe_limits[-1] if severe_limits else None
        
        print(f"   First rate limit: {first_limit[0].strftime('%H:%M:%S')} ({first_limit[1]}s wait)")
        
        if severe_limits:
            print(f"   Severe limits started: {severe_limits[0][0].strftime('%H:%M:%S')}")
            print(f"   Last severe limit: {last_severe[0].strftime('%H:%M:%S')} ({last_severe[1]:,}s = {last_severe[1]/3600:.1f} hours)")
            print(f"   Total severe events: {len(severe_limits)}")
        
        return events, severe_limits
        
    except Exception as e:
        print(f"❌ Error analyzing logs: {e}")
        return None, None

def calculate_safe_restart_time(events, severe_limits):
    """Calculate when it's safe to restart"""
    print()
    print("⏰ COOLDOWN CALCULATION:")
    print("-" * 30)
    
    current_time = datetime.now()
    
    # When we stopped processes (from emergency recovery)
    emergency_stop_time = datetime(2025, 9, 20, 11, 24, 17)  # From emergency_recovery.py output
    
    print(f"   Current time: {current_time.strftime('%H:%M:%S')}")
    print(f"   Emergency stop: {emergency_stop_time.strftime('%H:%M:%S')}")
    print(f"   Stopped ago: {current_time - emergency_stop_time}")
    
    if severe_limits:
        last_severe_time, last_severe_wait = severe_limits[-1]
        print(f"   Last severe limit: {last_severe_time.strftime('%H:%M:%S')} ({last_severe_wait:,}s wait)")
        
        # Spotify typically uses exponential backoff
        # After 85,000+ second waits, we need significant cooldown
        
        # Conservative approach: Wait at least 1 hour after stopping
        min_wait_after_stop = timedelta(hours=1)
        safe_time_from_stop = emergency_stop_time + min_wait_after_stop
        
        # Also wait based on last severe limit
        # Rule: Wait 10% of the last severe wait time, minimum 30 minutes
        cooldown_from_severe = max(timedelta(minutes=30), 
                                 timedelta(seconds=last_severe_wait * 0.1))
        safe_time_from_severe = last_severe_time + cooldown_from_severe
        
        # Take the later of the two
        safe_restart_time = max(safe_time_from_stop, safe_time_from_severe)
        
        print()
        print("🎯 SAFE RESTART TIMES:")
        print(f"   From emergency stop: {safe_time_from_stop.strftime('%H:%M:%S')}")
        print(f"   From severe limits: {safe_time_from_severe.strftime('%H:%M:%S')}")
        print(f"   Recommended: {safe_restart_time.strftime('%H:%M:%S')}")
        
        time_until_safe = safe_restart_time - current_time
        
        if time_until_safe.total_seconds() > 0:
            print()
            print(f"⏳ WAIT TIME REMAINING: {str(time_until_safe).split('.')[0]}")
            print(f"   Minutes remaining: {time_until_safe.total_seconds() / 60:.0f}")
            
            return safe_restart_time, time_until_safe
        else:
            print()
            print("✅ SAFE TO RESTART NOW!")
            return current_time, timedelta(0)
    else:
        # No severe limits, just use stop time + 30 minutes
        safe_time = emergency_stop_time + timedelta(minutes=30)
        time_until_safe = safe_time - current_time
        
        if time_until_safe.total_seconds() > 0:
            print(f"⏳ WAIT TIME: {str(time_until_safe).split('.')[0]}")
            return safe_time, time_until_safe
        else:
            print("✅ SAFE TO RESTART NOW!")
            return current_time, timedelta(0)

def provide_restart_guidance(safe_time, wait_remaining):
    """Provide guidance on when and how to restart"""
    print()
    print("🚀 RESTART GUIDANCE:")
    print("=" * 50)
    
    if wait_remaining.total_seconds() > 0:
        print("⏰ WAIT PERIOD:")
        print(f"   Restart after: {safe_time.strftime('%H:%M:%S')}")
        print(f"   Time remaining: {str(wait_remaining).split('.')[0]}")
        print("   Recommendation: Use this time to review strategy")
        print()
        print("📚 While waiting:")
        print("   • Review SMART_SCALING_GUIDE.md")
        print("   • Verify all phase scripts are ready")
        print("   • Plan monitoring approach")
    else:
        print("✅ READY TO START!")
        print("   API cooldown period complete")
        print()
        print("🧪 NEXT STEPS:")
        print("   1. Start conservative: ./start_phase1.sh")
        print("   2. Monitor closely: python phase_monitor.py")
        print("   3. Watch for rate limits in logs")
        print("   4. Scale up only if clean for 2+ hours")
    
    print()
    print("⚠️  IMPORTANT REMINDERS:")
    print("   • Start with Phase 1 (most conservative)")
    print("   • Any rate limit = immediate stop")
    print("   • Monitor every 30 minutes initially")
    print("   • Better slow and steady than fast and stuck")

def main():
    print("🕐 SPOTIFY API COOLDOWN CALCULATOR")
    print("=" * 60)
    print(f"Current Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Analyze rate limiting history
    events, severe_limits = analyze_rate_limit_timeline()
    
    if events:
        # Calculate safe restart time
        safe_time, wait_remaining = calculate_safe_restart_time(events, severe_limits)
        
        # Provide guidance
        provide_restart_guidance(safe_time, wait_remaining)
    else:
        print("⚠️  Could not analyze rate limit history")
        print("   Recommendation: Wait 1 hour from emergency stop (12:24)")
        print("   That would be safe to restart at: 12:24")
    
    print()
    print("=" * 60)

if __name__ == "__main__":
    main()