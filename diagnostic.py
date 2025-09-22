#!/usr/bin/env python3
"""
Quick diagnostic to check what data we actually have from our ETL processing
"""
import json
import os
from collections import Counter

# Check the main JSON file
print("=== CHECKING MAIN JSON FILE ===")
try:
    with open('/Users/iara/Projects/Zippotify_Datapipe/aggregated_music_data.json', 'r') as f:
        data = json.load(f)
    
    print(f"Total listen events: {data['metadata']['total_listen_events']}")
    print(f"Total users in metadata: {data['metadata']['total_users']}")
    print(f"User analytics keys: {list(data.get('user_analytics', {}).keys())}")
    print(f"Content analytics keys: {list(data.get('content_analytics', {}).keys())}")
    
except Exception as e:
    print(f"Error reading main JSON: {e}")

# Check if we have any non-empty CSV files
print("\n=== CHECKING CSV FILES ===")
csv_dir = '/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data'
for filename in os.listdir(csv_dir):
    if filename.endswith('.csv'):
        filepath = os.path.join(csv_dir, filename)
        try:
            with open(filepath, 'r') as f:
                lines = f.readlines()
            print(f"{filename}: {len(lines)} lines")
            if len(lines) > 1:
                print(f"  Sample: {lines[1].strip()}")
        except Exception as e:
            print(f"{filename}: Error - {e}")

print("\n=== CHECKING ORIGINAL ETL OUTPUT FILES ===")
# Check if our ETL pipeline created any data files we can use
output_files = [
    '/Users/iara/Projects/Zippotify_Datapipe/output/listen_events',
    '/Users/iara/Projects/Zippotify_Datapipe/output/auth_events',
    '/Users/iara/Projects/Zippotify_Datapipe/output/page_view_events',
    '/Users/iara/Projects/Zippotify_Datapipe/output/status_change_events'
]

for file_path in output_files:
    if os.path.exists(file_path):
        # Count lines
        with open(file_path, 'r') as f:
            line_count = sum(1 for line in f)
        print(f"{os.path.basename(file_path)}: {line_count:,} lines")
        
        # Check first few lines for data validation
        with open(file_path, 'r') as f:
            first_line = f.readline().strip()
        try:
            first_record = json.loads(first_line)
            print(f"  Sample keys: {list(first_record.keys())}")
        except:
            print("  Error parsing first line")
    else:
        print(f"{os.path.basename(file_path)}: File not found")
