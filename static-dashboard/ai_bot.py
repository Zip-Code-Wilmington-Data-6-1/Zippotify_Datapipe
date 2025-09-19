import pandas as pd
import json
from typing import Dict, Any, List
import random
from datetime import datetime

class DataInsightBot:
    def __init__(self):
        self.insights_cache = {}
        
    def generate_smart_insights(self, data: Dict[str, pd.DataFrame]) -> List[str]:
        """Generate intelligent insights from the dashboard data"""
        insights = []
        
        try:
            # Top artist insights
            if 'top_artists' in data and not data['top_artists'].empty:
                top_artist = data['top_artists'].iloc[0]
                second_artist = data['top_artists'].iloc[1] if len(data['top_artists']) > 1 else None
                
                insights.append(f"🎤 **{top_artist['artist']}** is dominating the charts with **{top_artist['play_count']:,} plays**!")
                
                if second_artist is not None:
                    gap = top_artist['play_count'] - second_artist['play_count']
                    insights.append(f"📈 {top_artist['artist']} leads by **{gap:,} plays** over {second_artist['artist']}")
            
            # Geographic insights
            if 'geographic_analysis' in data and not data['geographic_analysis'].empty:
                top_city = data['geographic_analysis'].iloc[0]
                total_cities = len(data['geographic_analysis'])
                insights.append(f"🌍 **{top_city['city']}, {top_city['state']}** leads in activity with **{top_city['total_plays']:,} plays** across {total_cities} cities")
                
                # Calculate concentration
                top_5_cities = data['geographic_analysis'].head(5)
                top_5_plays = top_5_cities['total_plays'].sum()
                total_plays = data['geographic_analysis']['total_plays'].sum()
                concentration = (top_5_plays / total_plays) * 100
                insights.append(f"📊 Top 5 cities account for **{concentration:.1f}%** of all listening activity")
            
            # Genre trends
            if 'genre_popularity' in data and not data['genre_popularity'].empty:
                top_genre = data['genre_popularity'].iloc[0]
                total_genres = len(data['genre_popularity'])
                insights.append(f"🎵 **{top_genre['genre'].title()}** genre is trending with **{top_genre['play_count']:,} plays** out of {total_genres} genres")
            
            # Song insights
            if 'top_songs' in data and not data['top_songs'].empty:
                top_song = data['top_songs'].iloc[0]
                insights.append(f"🎯 **'{top_song['song']}'** by {top_song['artist']} is the hottest track with **{top_song['play_count']:,} plays**")
                
                # Check for special characters (like Björk)
                special_artists = data['top_songs'][data['top_songs']['artist'].str.contains('ö|é|ü|ñ|ç', na=False)]
                if not special_artists.empty:
                    insights.append(f"🌍 International artists like **{special_artists['artist'].iloc[0]}** are gaining popularity globally")
            
            # Engagement patterns
            if 'hourly_patterns' in data and not data['hourly_patterns'].empty:
                peak_hour = data['hourly_patterns'].loc[data['hourly_patterns']['total_plays'].idxmax()]
                insights.append(f"⏰ Peak listening time is **{int(peak_hour['hour']):02d}:00** with **{peak_hour['total_plays']:,} plays**")
                
                # Find quiet hours
                quiet_hour = data['hourly_patterns'].loc[data['hourly_patterns']['total_plays'].idxmin()]
                insights.append(f"🌙 Quietest time is **{int(quiet_hour['hour']):02d}:00** with only **{quiet_hour['total_plays']:,} plays**")
            
            # Age demographics insights
            if 'age_distribution' in data and not data['age_distribution'].empty:
                total_users = data['age_distribution']['user_count'].sum()
                avg_age = (data['age_distribution']['age'] * data['age_distribution']['user_count']).sum() / total_users
                insights.append(f"👥 Average user age is **{avg_age:.1f} years** across **{total_users:,} users**")
                
                # Find most common age group
                peak_age = data['age_distribution'].loc[data['age_distribution']['user_count'].idxmax()]
                insights.append(f"🎂 Most active age group is **{peak_age['age']} years old** with **{peak_age['user_count']} users**")
            
        except Exception as e:
            insights.append(f"⚠️ Error generating insights: {str(e)}")
            
        return insights
    
    def answer_question(self, question: str, data: Dict[str, pd.DataFrame]) -> str:
        """Answer specific questions about the data"""
        question_lower = question.lower()
        
        try:
            if any(word in question_lower for word in ['artist', 'top artist', 'best artist']):
                if 'top_artists' in data and not data['top_artists'].empty:
                    top_artist = data['top_artists'].iloc[0]
                    return f"🎤 The top performing artist is **{top_artist['artist']}** with **{top_artist['play_count']:,} plays**. They're clearly dominating the charts right now!"
                    
            elif any(word in question_lower for word in ['song', 'track', 'music']):
                if 'top_songs' in data and not data['top_songs'].empty:
                    top_song = data['top_songs'].iloc[0]
                    return f"🎵 The hottest track is **'{top_song['song']}'** by **{top_song['artist']}** with **{top_song['play_count']:,} plays**. It's definitely the song everyone's talking about!"
                    
            elif any(word in question_lower for word in ['city', 'location', 'geographic', 'where']):
                if 'geographic_analysis' in data and not data['geographic_analysis'].empty:
                    top_city = data['geographic_analysis'].iloc[0]
                    return f"🌍 **{top_city['city']}, {top_city['state']}** is the most active city with **{top_city['total_plays']:,} total plays** from **{top_city['unique_users']} unique users**. That's some serious music love!"
                    
            elif any(word in question_lower for word in ['genre', 'style', 'type']):
                if 'genre_popularity' in data and not data['genre_popularity'].empty:
                    top_genre = data['genre_popularity'].iloc[0]
                    return f"🎨 **{top_genre['genre'].title()}** is the most popular genre with **{top_genre['play_count']:,} plays**. It's clearly what people are vibing to!"
                    
            elif any(word in question_lower for word in ['time', 'hour', 'when']):
                if 'hourly_patterns' in data and not data['hourly_patterns'].empty:
                    peak_hour = data['hourly_patterns'].loc[data['hourly_patterns']['total_plays'].idxmax()]
                    return f"⏰ Peak listening time is **{int(peak_hour['hour']):02d}:00** with **{peak_hour['total_plays']:,} plays**. That's when the party really gets started!"
                    
            elif any(word in question_lower for word in ['trend', 'pattern', 'insight']):
                insights = self.generate_smart_insights(data)
                return "📊 **Key Trends I'm Seeing:**\n\n" + "\n\n".join(insights[:3])
                
            else:
                # General summary
                insights = self.generate_smart_insights(data)
                return "🤖 **Here's what I'm seeing in your data:**\n\n" + "\n\n".join(insights[:4])
                
        except Exception as e:
            return f"⚠️ Sorry, I had trouble analyzing that. Error: {str(e)}"
    
    def get_suggested_questions(self) -> List[str]:
        """Return suggested questions users can ask"""
        return [
            "What are the current music trends?",
            "Which artist is performing best?",
            "What's the hottest song right now?",
            "Where are most listeners located?",
            "What genre is most popular?",
            "When do people listen to music most?",
            "Give me a summary of key insights",
            "What patterns do you see?"
        ]
    
    def get_random_insight(self, data: Dict[str, pd.DataFrame]) -> str:
        """Get a random interesting insight"""
        insights = self.generate_smart_insights(data)
        return random.choice(insights) if insights else "🤖 Still analyzing your data..."
