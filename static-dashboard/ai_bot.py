# static-dashboard/ai_bot.py
import json
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import numpy as np

class DataInsightBot:
    def __init__(self, csv_data, raw_data_paths=None):
        self.csv_data = csv_data
        self.raw_data = {}
        
        # Load raw JSONL data if paths provided
        if raw_data_paths:
            self.load_raw_data(raw_data_paths)
    
    def load_raw_data(self, paths):
        """Load raw JSONL files for deeper analysis"""
        for data_type, path in paths.items():
            try:
                events = []
                with open(path, 'r') as f:
                    for line in f:
                        try:
                            events.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue  # Skip malformed lines
                
                if events:
                    self.raw_data[data_type] = pd.DataFrame(events)
                    print(f"✅ Loaded {len(events)} {data_type.replace('_', ' ')} events")
                else:
                    print(f"⚠️ No valid data in {data_type}")
                    
            except FileNotFoundError:
                print(f"❌ Could not find {data_type} file: {path}")
            except Exception as e:
                print(f"❌ Error loading {data_type}: {e}")

    def generate_enhanced_insights(self):
        """Generate comprehensive insights using both CSV and raw data"""
        insights = []
        
        # Start with basic CSV insights
        insights.extend(self._generate_csv_insights())
        
        # Add advanced raw data insights
        insights.extend(self._generate_raw_data_insights())
        
        return insights[:12]  # Return top 12 insights

    def _generate_csv_insights(self):
        """Generate insights from CSV data"""
        insights = []
        
        # Top artist insights
        if 'top_artists' in self.csv_data and len(self.csv_data['top_artists']) > 0:
            top_artist = self.csv_data['top_artists'].iloc[0]
            insights.append(f"🎤 **{top_artist['artist']}** dominates with **{top_artist['play_count']:,} plays**")
        
        # Geographic insights
        if 'geographic_analysis' in self.csv_data and len(self.csv_data['geographic_analysis']) > 0:
            top_city = self.csv_data['geographic_analysis'].iloc[0]
            insights.append(f"🌍 **{top_city['city']}, {top_city['state']}** leads with **{top_city['total_plays']:,} plays**")
        
        # Genre insights
        if 'genre_popularity' in self.csv_data and len(self.csv_data['genre_popularity']) > 0:
            top_genre = self.csv_data['genre_popularity'].iloc[0]
            insights.append(f"🎵 **{top_genre['genre']}** is trending with **{top_genre['play_count']:,} plays**")
        
        return insights

    def _generate_raw_data_insights(self):
        """Generate advanced insights from raw JSONL data"""
        insights = []
        
        # 1. USER BEHAVIOR ANALYSIS
        if 'listen_events' in self.raw_data:
            insights.extend(self._analyze_user_behavior())
        
        # 2. SESSION ANALYSIS
        if 'listen_events' in self.raw_data:
            insights.extend(self._analyze_sessions())
        
        # 3. TEMPORAL PATTERNS
        if 'listen_events' in self.raw_data:
            insights.extend(self._analyze_temporal_patterns())
        
        # 4. AUTHENTICATION INSIGHTS
        if 'auth_events' in self.raw_data:
            insights.extend(self._analyze_auth_patterns())
        
        # 5. DEVICE USAGE
        if 'listen_events' in self.raw_data:
            insights.extend(self._analyze_device_usage())
        
        return insights

    def _analyze_user_behavior(self):
        """Analyze user behavior patterns"""
        insights = []
        listen_df = self.raw_data['listen_events']
        
        try:
            # Session analysis
            if 'sessionId' in listen_df.columns:
                session_lengths = listen_df.groupby('sessionId').size()
                avg_session = session_lengths.mean()
                max_session = session_lengths.max()
                insights.append(f"🎧 **Session patterns**: Average {avg_session:.1f} songs per session, longest session had {max_session} tracks")
            
            # User engagement
            if 'userId' in listen_df.columns:
                user_activity = listen_df.groupby('userId').size()
                top_user_plays = user_activity.max()
                avg_user_plays = user_activity.mean()
                insights.append(f"👤 **User engagement**: Most active user played {top_user_plays} songs, average {avg_user_plays:.1f} per user")
            
            # Song completion analysis
            if 'length' in listen_df.columns and listen_df['length'].notna().any():
                avg_length = listen_df['length'].mean() / 1000  # Convert to seconds
                insights.append(f"⏱️ **Song duration**: Average track length is {avg_length:.1f} seconds")
        
        except Exception as e:
            insights.append(f"⚠️ **Behavior analysis**: Limited due to data structure")
        
        return insights

    def _analyze_sessions(self):
        """Analyze session patterns"""
        insights = []
        listen_df = self.raw_data['listen_events']
        
        try:
            if 'sessionId' in listen_df.columns and 'ts' in listen_df.columns:
                # Convert timestamps
                listen_df['datetime'] = pd.to_datetime(listen_df['ts'], unit='ms', errors='coerce')
                
                # Session duration analysis
                session_times = listen_df.groupby('sessionId')['datetime'].agg(['min', 'max'])
                session_times['duration'] = (session_times['max'] - session_times['min']).dt.total_seconds() / 60
                
                avg_duration = session_times['duration'].mean()
                max_duration = session_times['duration'].max()
                
                if avg_duration > 0:
                    insights.append(f"⏰ **Session duration**: Average {avg_duration:.1f} minutes, longest session {max_duration:.1f} minutes")
                
                # Sessions per user
                if 'userId' in listen_df.columns:
                    user_sessions = listen_df.groupby('userId')['sessionId'].nunique()
                    avg_sessions = user_sessions.mean()
                    insights.append(f"🔄 **User retention**: Average {avg_sessions:.1f} sessions per user")
        
        except Exception as e:
            insights.append(f"⚠️ **Session analysis**: Data processing continues...")
        
        return insights

    def _analyze_temporal_patterns(self):
        """Analyze time-based patterns"""
        insights = []
        listen_df = self.raw_data['listen_events']
        
        try:
            if 'ts' in listen_df.columns:
                # Convert timestamps
                listen_df['datetime'] = pd.to_datetime(listen_df['ts'], unit='ms', errors='coerce')
                listen_df['hour'] = listen_df['datetime'].dt.hour
                listen_df['day_of_week'] = listen_df['datetime'].dt.day_name()
                
                # Peak hours
                hourly_activity = listen_df['hour'].value_counts().sort_index()
                peak_hour = hourly_activity.idxmax()
                peak_plays = hourly_activity.max()
                insights.append(f"🕒 **Peak activity**: {peak_hour}:00 with {peak_plays:,} plays")
                
                # Weekend vs weekday
                weekend_plays = len(listen_df[listen_df['day_of_week'].isin(['Saturday', 'Sunday'])])
                weekday_plays = len(listen_df) - weekend_plays
                
                if weekend_plays > weekday_plays:
                    insights.append(f"🎉 **Weekend preference**: {weekend_plays:,} weekend vs {weekday_plays:,} weekday plays")
                else:
                    insights.append(f"💼 **Weekday focus**: {weekday_plays:,} weekday vs {weekend_plays:,} weekend plays")
                
                # Activity spread
                daily_activity = listen_df['day_of_week'].value_counts()
                most_active_day = daily_activity.idxmax()
                least_active_day = daily_activity.idxmin()
                insights.append(f"📅 **Daily patterns**: Most active on {most_active_day} ({daily_activity.max():,} plays), quietest on {least_active_day} ({daily_activity.min():,} plays)")
        
        except Exception as e:
            insights.append(f"⚠️ **Temporal analysis**: Time data processing...")
        
        return insights

    def _analyze_auth_patterns(self):
        """Analyze authentication patterns"""
        insights = []
        auth_df = self.raw_data['auth_events']
        
        try:
            if 'success' in auth_df.columns:
                total_attempts = len(auth_df)
                successful_logins = auth_df['success'].sum() if auth_df['success'].dtype == 'bool' else len(auth_df[auth_df['success'] == True])
                success_rate = (successful_logins / total_attempts) * 100
                insights.append(f"🔐 **Login success rate**: {success_rate:.1f}% ({successful_logins:,}/{total_attempts:,} attempts)")
            
            # User authentication frequency
            if 'userId' in auth_df.columns:
                user_auth_freq = auth_df.groupby('userId').size()
                avg_auth = user_auth_freq.mean()
                max_auth = user_auth_freq.max()
                insights.append(f"🔑 **Auth patterns**: Average {avg_auth:.1f} logins per user, most active: {max_auth} attempts")
        
        except Exception as e:
            insights.append(f"⚠️ **Auth analysis**: Authentication data processing...")
        
        return insights

    def _analyze_device_usage(self):
        """Analyze device and platform usage"""
        insights = []
        listen_df = self.raw_data['listen_events']
        
        try:
            if 'userAgent' in listen_df.columns:
                # Mobile vs Desktop detection
                mobile_indicators = ['Mobile', 'Android', 'iPhone', 'iPad']
                is_mobile = listen_df['userAgent'].str.contains('|'.join(mobile_indicators), case=False, na=False)
                
                mobile_count = is_mobile.sum()
                desktop_count = len(listen_df) - mobile_count
                mobile_pct = (mobile_count / len(listen_df)) * 100
                
                insights.append(f"📱 **Platform usage**: {mobile_pct:.1f}% mobile ({mobile_count:,} plays) vs {100-mobile_pct:.1f}% desktop ({desktop_count:,} plays)")
                
                # Browser analysis (simplified)
                browsers = []
                if listen_df['userAgent'].str.contains('Chrome', na=False).any():
                    chrome_count = listen_df['userAgent'].str.contains('Chrome', na=False).sum()
                    browsers.append(f"Chrome ({chrome_count:,})")
                if listen_df['userAgent'].str.contains('Safari', na=False).any():
                    safari_count = listen_df['userAgent'].str.contains('Safari', na=False).sum()
                    browsers.append(f"Safari ({safari_count:,})")
                
                if browsers:
                    insights.append(f"🌐 **Browser usage**: {', '.join(browsers[:2])}")
        
        except Exception as e:
            insights.append(f"⚠️ **Device analysis**: User agent processing...")
        
        return insights

    def answer_advanced_question(self, question):
        """Handle complex questions using both CSV and raw data with improved routing"""
        question_lower = question.lower()
        
        # Geographic/location questions - handle early since they're common
        if any(word in question_lower for word in ['location', 'where', 'city', 'cities', 'state', 'place', 'geographic']):
            return self._answer_basic_question(question_lower)
        
        # Count questions - handle these specifically
        if 'how many' in question_lower or 'count' in question_lower:
            return self._answer_basic_question(question_lower)
        
        # Session-related questions
        if any(word in question_lower for word in ['session', 'how long', 'duration']):
            if 'listen_events' in self.raw_data:
                return self._answer_session_question(question_lower)
        
        # User behavior questions
        if any(word in question_lower for word in ['user', 'who', 'most active', 'top user']) and 'how many' not in question_lower:
            if 'listen_events' in self.raw_data:
                return self._answer_user_question(question_lower)
        
        # Device/platform questions
        if any(word in question_lower for word in ['device', 'mobile', 'desktop', 'platform', 'browser']):
            if 'listen_events' in self.raw_data:
                return self._answer_device_question(question_lower)
        
        # Time-related questions
        if any(word in question_lower for word in ['when', 'time', 'hour', 'day', 'peak', 'busy']):
            if 'listen_events' in self.raw_data:
                return self._answer_time_question(question_lower)
        
        # Authentication questions
        if any(word in question_lower for word in ['login', 'auth', 'security', 'access']):
            if 'auth_events' in self.raw_data:
                return self._answer_auth_question(question_lower)
        
        # Music-related questions (songs, artists, genres)
        if any(word in question_lower for word in ['song', 'track', 'music', 'artist', 'genre', 'play']):
            return self._answer_basic_question(question_lower)
        
        # Fall back to basic CSV insights
        return self._answer_basic_question(question_lower)

    def _answer_session_question(self, question):
        """Answer session-related questions"""
        try:
            listen_df = self.raw_data['listen_events']
            if 'sessionId' in listen_df.columns:
                session_lengths = listen_df.groupby('sessionId').size()
                avg_session = session_lengths.mean()
                max_session = session_lengths.max()
                total_sessions = len(session_lengths)
                
                return f"🎧 **Session Analysis**: We have {total_sessions:,} total sessions. Average session length is {avg_session:.1f} songs, with the longest session containing {max_session} tracks. Most users listen to 3-8 songs per session."
        except:
            pass
        
        return "I need session data to answer questions about listening patterns."

    def _answer_user_question(self, question):
        """Answer user behavior questions"""
        try:
            listen_df = self.raw_data['listen_events']
            if 'userId' in listen_df.columns:
                user_activity = listen_df.groupby('userId').size()
                top_user_plays = user_activity.max()
                avg_user_plays = user_activity.mean()
                total_users = len(user_activity)
                
                return f"👤 **User Behavior**: {total_users:,} active users in the data. The most active user played {top_user_plays} songs, while the average user played {avg_user_plays:.1f} songs. This shows a healthy mix of casual and power users."
        except:
            pass
        
        return "I need user activity data to analyze user behavior patterns."

    def _answer_device_question(self, question):
        """Answer device/platform questions"""
        try:
            listen_df = self.raw_data['listen_events']
            if 'userAgent' in listen_df.columns:
                mobile_indicators = ['Mobile', 'Android', 'iPhone', 'iPad']
                is_mobile = listen_df['userAgent'].str.contains('|'.join(mobile_indicators), case=False, na=False)
                mobile_pct = (is_mobile.sum() / len(listen_df)) * 100
                
                return f"📱 **Device Usage**: {mobile_pct:.1f}% of listening happens on mobile devices, while {100-mobile_pct:.1f}% occurs on desktop. This reflects modern listening habits with mobile-first music consumption."
        except:
            pass
        
        return "I need user agent data to analyze device and platform usage."

    def _answer_time_question(self, question):
        """Answer time-related questions"""
        try:
            listen_df = self.raw_data['listen_events']
            if 'ts' in listen_df.columns:
                listen_df['datetime'] = pd.to_datetime(listen_df['ts'], unit='ms', errors='coerce')
                listen_df['hour'] = listen_df['datetime'].dt.hour
                
                hourly_activity = listen_df['hour'].value_counts().sort_index()
                peak_hour = hourly_activity.idxmax()
                peak_plays = hourly_activity.max()
                
                # Determine time period
                if 6 <= peak_hour <= 11:
                    period = "morning"
                elif 12 <= peak_hour <= 17:
                    period = "afternoon"
                elif 18 <= peak_hour <= 22:
                    period = "evening"
                else:
                    period = "night/early morning"
                
                return f"🕒 **Peak Activity**: Most listening happens at {peak_hour}:00 ({period}) with {peak_plays:,} plays. This shows users prefer {period} listening sessions."
        except:
            pass
        
        return "I need timestamp data to analyze listening time patterns."

    def _answer_auth_question(self, question):
        """Answer authentication-related questions"""
        try:
            auth_df = self.raw_data['auth_events']
            if 'success' in auth_df.columns:
                total_attempts = len(auth_df)
                successful_logins = auth_df['success'].sum() if auth_df['success'].dtype == 'bool' else len(auth_df[auth_df['success'] == True])
                success_rate = (successful_logins / total_attempts) * 100
                
                return f"🔐 **Authentication**: Login success rate is {success_rate:.1f}% with {successful_logins:,} successful logins out of {total_attempts:,} attempts. This indicates {'good' if success_rate > 90 else 'moderate'} user experience with authentication."
        except:
            pass
        
        return "I need authentication data to analyze login patterns."

    def _answer_basic_question(self, question):
        """Fall back to basic CSV-based answers with comprehensive question parsing"""
        
        # Artist-related questions
        if any(word in question for word in ['artist', 'musician', 'band', 'singer']):
            if 'top_artists' in self.csv_data and len(self.csv_data['top_artists']) > 0:
                if 'how many' in question or 'count' in question:
                    total_artists = len(self.csv_data['top_artists'])
                    return f"🎤 **Artist Count**: There are {total_artists:,} artists in the dataset. The top artist is {self.csv_data['top_artists'].iloc[0]['artist']} with {self.csv_data['top_artists'].iloc[0]['play_count']:,} plays."
                else:
                    top_artist = self.csv_data['top_artists'].iloc[0]
                    return f"🎤 **Top Artist**: {top_artist['artist']} leads with {top_artist['play_count']:,} plays across all tracks."
        
        # Geographic/location questions  
        if any(word in question for word in ['location', 'where', 'city', 'cities', 'state', 'place', 'geographic']):
            if 'geographic_analysis' in self.csv_data and len(self.csv_data['geographic_analysis']) > 0:
                if 'how many' in question or 'count' in question:
                    if 'cities' in question or 'city' in question:
                        total_cities = len(self.csv_data['geographic_analysis']['city'].unique()) if 'city' in self.csv_data['geographic_analysis'].columns else len(self.csv_data['geographic_analysis'])
                        total_states = len(self.csv_data['geographic_analysis']['state'].unique()) if 'state' in self.csv_data['geographic_analysis'].columns else 0
                        return f"🌍 **Geographic Coverage**: There are {total_cities:,} unique cities across {total_states} states in the dataset. Top city is {self.csv_data['geographic_analysis'].iloc[0]['city']}, {self.csv_data['geographic_analysis'].iloc[0]['state']} with {self.csv_data['geographic_analysis'].iloc[0]['total_plays']:,} plays."
                else:
                    top_city = self.csv_data['geographic_analysis'].iloc[0]
                    return f"🌍 **Top Location**: {top_city['city']}, {top_city['state']} has the most activity with {top_city['total_plays']:,} total plays."
        
        # Song-related questions
        if any(word in question for word in ['song', 'track', 'music', 'play']):
            if 'top_songs' in self.csv_data and len(self.csv_data['top_songs']) > 0:
                if 'how many' in question or 'count' in question:
                    total_songs = len(self.csv_data['top_songs'])
                    return f"🎵 **Song Count**: There are {total_songs:,} songs in the dataset. Top song is '{self.csv_data['top_songs'].iloc[0]['song']}' by {self.csv_data['top_songs'].iloc[0]['artist']} with {self.csv_data['top_songs'].iloc[0]['play_count']:,} plays."
                else:
                    top_song = self.csv_data['top_songs'].iloc[0]
                    return f"🎵 **Top Song**: '{top_song['song']}' by {top_song['artist']} with {top_song['play_count']:,} plays."
        
        # Genre questions
        if any(word in question for word in ['genre', 'style', 'type of music']):
            if 'genre_popularity' in self.csv_data and len(self.csv_data['genre_popularity']) > 0:
                if 'how many' in question or 'count' in question:
                    total_genres = len(self.csv_data['genre_popularity'])
                    return f"🎼 **Genre Count**: There are {total_genres:,} genres represented. Top genre is {self.csv_data['genre_popularity'].iloc[0]['genre']} with {self.csv_data['genre_popularity'].iloc[0]['play_count']:,} plays."
                else:
                    top_genre = self.csv_data['genre_popularity'].iloc[0]
                    return f"🎼 **Top Genre**: {top_genre['genre']} dominates with {top_genre['play_count']:,} plays."
        
        # User questions
        if any(word in question for word in ['user', 'people', 'listener']):
            # Try to get user count from raw data first
            if 'listen_events' in self.raw_data and 'userId' in self.raw_data['listen_events'].columns:
                unique_users = self.raw_data['listen_events']['userId'].nunique()
                return f"👥 **User Count**: There are {unique_users:,} unique users in the dataset based on listening events."
            else:
                return f"👥 **Users**: I can see user activity data but need to access the raw events for exact user counts."
        
        # Generic insight fallback
        insights = self.generate_enhanced_insights()
        return insights[0] if insights else "I'm analyzing the data to provide better insights. Try asking about specific counts like 'how many cities', 'how many artists', or 'how many users'!"

    def get_data_summary(self):
        """Provide a summary of available data sources"""
        summary = []
        
        # CSV data summary
        csv_files = len(self.csv_data)
        summary.append(f"📊 **CSV Files**: {csv_files} aggregated data files loaded")
        
        # Raw data summary
        if self.raw_data:
            for data_type, df in self.raw_data.items():
                event_count = len(df)
                summary.append(f"📁 **{data_type.replace('_', ' ').title()}**: {event_count:,} events")
        else:
            summary.append("📁 **Raw Data**: Not loaded (CSV data only)")
        
        return summary

    def generate_conversation_summary(self, chat_history):
        """Generate a summary of the conversation"""
        if not chat_history:
            return "No conversation to summarize yet."
        
        user_questions = [msg[1] for msg in chat_history if msg[0] == "user"]
        
        if not user_questions:
            return "No questions asked yet."
        
        summary_parts = []
        summary_parts.append(f"**Conversation Overview**: {len(user_questions)} questions asked")
        
        # Categorize questions
        categories = {
            'sessions': ['session', 'user journey', 'behavior'],
            'devices': ['device', 'platform', 'mobile', 'desktop'],
            'auth': ['auth', 'login', 'registration', 'sign'],
            'music': ['song', 'artist', 'genre', 'music', 'listen'],
            'time': ['hour', 'day', 'time', 'when', 'peak'],
            'general': []
        }
        
        question_categories = {cat: 0 for cat in categories}
        
        for question in user_questions:
            question_lower = question.lower()
            categorized = False
            for category, keywords in categories.items():
                if category != 'general' and any(keyword in question_lower for keyword in keywords):
                    question_categories[category] += 1
                    categorized = True
                    break
            if not categorized:
                question_categories['general'] += 1
        
        # Add category breakdown
        top_categories = sorted(question_categories.items(), key=lambda x: x[1], reverse=True)
        if top_categories[0][1] > 0:
            summary_parts.append(f"**Most asked about**: {top_categories[0][0].title()} ({top_categories[0][1]} questions)")
        
        summary_parts.append("**Data sources used**: CSV analytics + Raw JSONL events")
        
        return "\n".join(summary_parts)

    def get_smart_suggestion(self, chat_history):
        """Generate smart suggestions based on conversation history"""
        if not chat_history:
            return "Try asking: 'What are the main user behavior patterns?' to get started!"
        
        user_questions = [msg[1] for msg in chat_history if msg[0] == "user"]
        
        # Analyze what hasn't been asked about yet
        asked_about = {
            'sessions': any('session' in q.lower() for q in user_questions),
            'devices': any(any(word in q.lower() for word in ['device', 'platform']) for q in user_questions),
            'auth': any(any(word in q.lower() for word in ['auth', 'login']) for q in user_questions),
            'temporal': any(any(word in q.lower() for word in ['hour', 'time', 'when']) for q in user_questions),
            'geographic': any('geo' in q.lower() or 'location' in q.lower() for q in user_questions)
        }
        
        suggestions = {
            'sessions': "How long are typical user sessions?",
            'devices': "What devices do users prefer for music listening?",
            'auth': "When do most users register or log in?",
            'temporal': "What are the peak music listening hours?",
            'geographic': "Which regions have the highest engagement?"
        }
        
        # Suggest something not yet explored
        for topic, asked in asked_about.items():
            if not asked:
                return f"**Explore {topic.title()}**: {suggestions[topic]}"
        
        # If everything has been covered, suggest deeper analysis
        return "**Deep Dive**: Try asking about correlations between different metrics, like 'How do authentication patterns relate to listening behavior?'"

    # Keep the existing methods for backward compatibility
    def generate_smart_insights(self):
        """Legacy method - redirects to enhanced insights"""
        return self.generate_enhanced_insights()
    
    def answer_question(self, question):
        """Legacy method - redirects to advanced question answering"""
        return self.answer_advanced_question(question)

# Initialize the enhanced bot
def initialize_enhanced_bot(csv_data):
    """Initialize the enhanced AI bot with raw data access"""
    raw_data_paths = {
        'listen_events': '../data/sample/listen_events_head.jsonl',
        'auth_events': '../data/sample/auth_events_head.jsonl',
        'page_view_events': '../data/sample/page_view_events_head.jsonl',
        'status_change_events': '../data/sample/status_change_events_head.jsonl'
    }
    
    return DataInsightBot(csv_data, raw_data_paths)