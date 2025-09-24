#!/usr/bin/env python3
"""
Test script to verify OpenAI integration works with the SimpleDataBot
"""

import json
import pandas as pd
import openai
import os
import sys
sys.path.append('.')

# Add the dashboard directory to the path so we can import
from dashboard import SimpleDataBot

def test_openai_integration():
    """Test the OpenAI integration with real data"""
    print("🧪 Testing TracktionAI OpenAI Integration...")
    
    # Load data like the dashboard does
    try:
        with open('aggregated_music_data.json', 'r') as f:
            aggregated_data = json.load(f)
        print("✅ Aggregated data loaded")
        
        csv_data = {
            'geographic_analysis': pd.read_csv('aggregated_data/geographic_analysis.csv'),
            'top_artists': pd.read_csv('aggregated_data/top_artists.csv'),
            'top_songs': pd.read_csv('aggregated_data/top_songs.csv'),
            'hourly_patterns': pd.read_csv('aggregated_data/hourly_patterns.csv'),
            'age_distribution': pd.read_csv('aggregated_data/age_distribution.csv'),
            'engagement_by_level': pd.read_csv('aggregated_data/engagement_by_level.csv')
        }
        print("✅ CSV data loaded")
        
        # Create bot instance
        bot = SimpleDataBot(csv_data, aggregated_data)
        print(f"✅ Bot created, OpenAI enabled: {bot.use_openai}")
        
        if bot.use_openai:
            # Test with the question that was failing
            test_question = "What are the top genres?"
            print(f"\n🤖 Testing question: '{test_question}'")
            
            try:
                answer = bot.answer_question(test_question)
                print(f"✅ Answer received: {answer[:100]}...")
                return True
            except Exception as e:
                print(f"❌ Error getting answer: {e}")
                return False
        else:
            print("⚠️ OpenAI not available - check API key")
            return False
            
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return False

if __name__ == "__main__":
    success = test_openai_integration()
    print(f"\n{'🎉 Test PASSED' if success else '❌ Test FAILED'}")
