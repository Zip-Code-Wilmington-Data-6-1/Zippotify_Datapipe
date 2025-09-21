#!/usr/bin/env python3
"""Stress test for AI bot edge cases"""

import pandas as pd
import sys
import os

# Add the current directory to sys.path so we can import ai_bot
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ai_bot import DataInsightBot

def test_edge_cases():
    """Test edge cases that might trigger pandas Series errors"""
    print("🧪 Testing AI Bot Edge Cases...")
    bot = DataInsightBot()
    
    # Test with empty dataframes
    empty_data = {
        'top_artists': pd.DataFrame(),
        'top_songs': pd.DataFrame(),
        'genre_popularity': pd.DataFrame(),
        'geographic_analysis': pd.DataFrame(),
        'hourly_patterns': pd.DataFrame(),
        'age_distribution': pd.DataFrame()
    }
    
    print("\n📊 Testing with empty dataframes...")
    try:
        insights = bot.generate_smart_insights(empty_data)
        print(f"✅ Empty data handled: {len(insights)} insights")
        for insight in insights:
            print(f"   {insight}")
    except Exception as e:
        print(f"❌ Error with empty data: {e}")
    
    # Test with minimal dataframes (1 row each)
    minimal_data = {
        'top_artists': pd.DataFrame({'artist': ['Test Artist'], 'play_count': [100]}),
        'top_songs': pd.DataFrame({'song': ['Test Song'], 'artist': ['Test Artist'], 'play_count': [50]}),
        'genre_popularity': pd.DataFrame({'genre': ['test'], 'play_count': [75]}),
        'geographic_analysis': pd.DataFrame({
            'city': ['Test City'], 
            'state': ['TS'], 
            'total_plays': [200],
            'unique_users': [10]
        }),
        'hourly_patterns': pd.DataFrame({'hour': [12], 'total_plays': [300]}),
        'age_distribution': pd.DataFrame({'age': [25], 'user_count': [50]})
    }
    
    print("\n📊 Testing with minimal dataframes...")
    try:
        insights = bot.generate_smart_insights(minimal_data)
        print(f"✅ Minimal data handled: {len(insights)} insights")
        for insight in insights[:3]:
            print(f"   {insight}")
    except Exception as e:
        print(f"❌ Error with minimal data: {e}")
    
    # Test with special characters data
    special_chars_data = {
        'top_artists': pd.DataFrame({'artist': ['Björk', 'Céline Dion'], 'play_count': [100, 90]}),
        'top_songs': pd.DataFrame({
            'song': ['Héroes', 'Naïve'], 
            'artist': ['Björk', 'Céline Dion'], 
            'play_count': [50, 45]
        }),
        'genre_popularity': pd.DataFrame({'genre': ['pop'], 'play_count': [75]}),
        'geographic_analysis': pd.DataFrame({
            'city': ['São Paulo'], 
            'state': ['SP'], 
            'total_plays': [200],
            'unique_users': [10]
        }),
        'hourly_patterns': pd.DataFrame({'hour': [12], 'total_plays': [300]}),
        'age_distribution': pd.DataFrame({'age': [25], 'user_count': [50]})
    }
    
    print("\n🌍 Testing with special characters...")
    try:
        insights = bot.generate_smart_insights(special_chars_data)
        print(f"✅ Special chars handled: {len(insights)} insights")
        for insight in insights[:3]:
            print(f"   {insight}")
    except Exception as e:
        print(f"❌ Error with special characters: {e}")
    
    # Test all question types
    test_questions = [
        "What are the current music trends?",
        "Which artist is performing best?",
        "What's the hottest song right now?",
        "Where are most listeners located?",
        "What genre is most popular?",
        "When do people listen to music most?",
        "Give me insights",
        "Something completely random"
    ]
    
    print("\n❓ Testing all question types...")
    for question in test_questions:
        try:
            answer = bot.answer_question(question, special_chars_data)
            print(f"✅ {question[:30]}... -> {len(answer)} chars")
        except Exception as e:
            print(f"❌ Error on '{question}': {e}")
    
    print("\n🎲 Testing random insights multiple times...")
    for i in range(5):
        try:
            insight = bot.get_random_insight(special_chars_data)
            print(f"✅ Random {i+1}: {insight[:50]}...")
        except Exception as e:
            print(f"❌ Error on random insight {i+1}: {e}")

if __name__ == "__main__":
    test_edge_cases()
