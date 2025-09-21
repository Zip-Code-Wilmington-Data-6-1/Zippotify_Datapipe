#!/usr/bin/env python3
"""
Test script to verify the AI bot fixes work correctly
"""
import sys
import os

# Add the static-dashboard directory to the Python path
sys.path.append('/Users/iara/Projects/Zippotify_Datapipe/static-dashboard')

from ai_bot import DataInsightBot
import json
import pandas as pd

def load_test_csv_data():
    """Load sample CSV data for testing"""
    # Load the aggregated data
    data_path = '/Users/iara/Projects/Zippotify_Datapipe/static-dashboard/aggregated_data'
    
    csv_data = {}
    
    # Load geographic analysis
    try:
        csv_data['geographic_analysis'] = pd.read_csv(f"{data_path}/geographic_analysis.csv")
        print(f"✅ Loaded geographic data: {len(csv_data['geographic_analysis'])} records")
    except Exception as e:
        print(f"❌ Could not load geographic data: {e}")
    
    # Load top artists  
    try:
        csv_data['top_artists'] = pd.read_csv(f"{data_path}/top_artists.csv")
        print(f"✅ Loaded artists data: {len(csv_data['top_artists'])} records")
    except Exception as e:
        print(f"❌ Could not load artists data: {e}")
    
    # Load top songs
    try:
        csv_data['top_songs'] = pd.read_csv(f"{data_path}/top_songs.csv")
        print(f"✅ Loaded songs data: {len(csv_data['top_songs'])} records")
    except Exception as e:
        print(f"❌ Could not load songs data: {e}")
    
    # Load genre popularity
    try:
        csv_data['genre_popularity'] = pd.read_csv(f"{data_path}/genre_popularity.csv")
        print(f"✅ Loaded genre data: {len(csv_data['genre_popularity'])} records")
    except Exception as e:
        print(f"❌ Could not load genre data: {e}")
        
    return csv_data

def test_ai_bot():
    """Test the AI bot with various questions"""
    
    print("🤖 Testing Enhanced AI Bot...")
    print("=" * 50)
    
    # Load test data
    csv_data = load_test_csv_data()
    
    # Initialize bot with raw data
    raw_data_paths = {
        'listen_events': '/Users/iara/Projects/Zippotify_Datapipe/data/sample/listen_events_head.jsonl',
        'auth_events': '/Users/iara/Projects/Zippotify_Datapipe/data/sample/auth_events_head.jsonl',
        'page_view_events': '/Users/iara/Projects/Zippotify_Datapipe/data/sample/page_view_events_head.jsonl',
        'status_change_events': '/Users/iara/Projects/Zippotify_Datapipe/data/sample/status_change_events_head.jsonl'
    }
    
    bot = DataInsightBot(csv_data, raw_data_paths)
    
    # Test questions
    test_questions = [
        "how many cities are there in the data sample?",
        "how many artists are in the dataset?", 
        "what's the top city?",
        "how many users are there?",
        "how many songs are available?"
    ]
    
    print("\n🧪 Testing Questions:")
    print("-" * 30)
    
    for question in test_questions:
        print(f"\n❓ Q: {question}")
        try:
            response = bot.answer_question(question)
            print(f"✅ A: {response}")
        except Exception as e:
            print(f"❌ Error: {e}")
    
    print("\n✅ AI Bot testing complete!")

if __name__ == "__main__":
    test_ai_bot()
