#!/usr/bin/env python3
"""
Fix the metadata date range to reflect the actual data span
"""
import pandas as pd
import json
from datetime import datetime

def fix_metadata():
    print("🔍 Analyzing actual date range in your dataset...")
    
    # Read the daily active users CSV to get actual date range
    daily_df = pd.read_csv('aggregated_data/daily_active_users.csv')
    actual_start = daily_df['date'].min()
    actual_end = daily_df['date'].max()
    total_days = len(daily_df)
    
    print(f"📅 **Actual Date Range Found:**")
    print(f"   From: {actual_start}")
    print(f"   To: {actual_end}")
    print(f"   Total days: {total_days}")
    print(f"   Duration: ~{total_days/365:.1f} years")
    
    # Load and update the metadata
    print(f"\n🔧 Updating metadata...")
    with open('aggregated_music_data.json', 'r') as f:
        data = json.load(f)
    
    # Show what metadata currently says
    current_start = data['metadata']['date_range']['start']
    current_end = data['metadata']['date_range']['end']
    print(f"   Current metadata shows: {current_start} to {current_end}")
    
    # Update the metadata with correct dates
    data['metadata']['date_range']['start'] = actual_start
    data['metadata']['date_range']['end'] = actual_end
    data['metadata']['generated_at'] = datetime.now().isoformat()
    
    # Save the updated metadata
    with open('aggregated_music_data.json', 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"   ✅ Updated metadata to: {actual_start} to {actual_end}")
    print(f"\n🎉 Metadata fixed! Your dashboard now shows the correct 2-year date range.")
    
    return actual_start, actual_end, total_days

if __name__ == "__main__":
    fix_metadata()
