# AI Bot Integration Guide for TracktionAI Dashboard

## Overview
Adding an AI bot to provide real-time insights about your music streaming data will enhance user experience and provide intelligent analysis.

## Option 1: OpenAI GPT Integration (Recommended)

### Prerequisites
- OpenAI API key
- `openai` Python package

### Implementation Steps

1. **Install dependencies**:
```bash
pip install openai streamlit-chat
```

2. **Add to requirements.txt**:
```
openai>=1.0.0
streamlit-chat>=0.1.1
```

3. **Create AI bot module** (`static-dashboard/ai_bot.py`):
```python
import openai
import pandas as pd
import json
from typing import Dict, Any

class MusicDataBot:
    def __init__(self, api_key: str):
        self.client = openai.OpenAI(api_key=api_key)
        
    def analyze_data(self, user_query: str, data_context: Dict[str, Any]) -> str:
        """Generate insights based on user query and current data"""
        
        # Create context from your dashboard data
        context = self._create_data_context(data_context)
        
        system_prompt = f"""
        You are an AI music analytics expert for TracktionAI. 
        You have access to real-time music streaming data and should provide insights based on:
        
        Current Data Context:
        {context}
        
        Provide concise, actionable insights. Include specific numbers when relevant.
        Focus on trends, patterns, and business recommendations.
        """
        
        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query}
            ],
            temperature=0.7,
            max_tokens=500
        )
        
        return response.choices[0].message.content
    
    def _create_data_context(self, data: Dict[str, Any]) -> str:
        """Convert dashboard data into text context for AI"""
        context_parts = []
        
        if 'top_artists' in data:
            top_artists = data['top_artists'].head(5)
            context_parts.append(f"Top 5 Artists: {list(top_artists['artist'])}")
            
        if 'top_songs' in data:
            top_songs = data['top_songs'].head(5)
            context_parts.append(f"Top 5 Songs: {list(top_songs['song'])}")
            
        if 'genre_popularity' in data:
            top_genres = data['genre_popularity'].head(3)
            context_parts.append(f"Top 3 Genres: {list(top_genres['genre'])}")
            
        if 'geographic_analysis' in data:
            top_cities = data['geographic_analysis'].head(3)
            context_parts.append(f"Top 3 Cities: {list(top_cities['city'])}")
            
        return "\n".join(context_parts)

    def get_suggested_questions(self) -> list:
        """Return suggested questions users can ask"""
        return [
            "What are the current music trends?",
            "Which artist is performing best this week?",
            "What insights do you see in the geographic data?",
            "How is user engagement changing over time?",
            "What genres are trending up or down?",
            "Which cities have the most active users?",
            "What patterns do you see in listening behavior?",
            "Give me a summary of key metrics"
        ]
```

4. **Add bot to dashboard** (modify `dashboard.py`):
```python
# Add imports at the top
import streamlit as st
from streamlit_chat import message
from ai_bot import MusicDataBot
import os

# Add after existing imports
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'bot' not in st.session_state:
    # Initialize bot (you'll need to set OPENAI_API_KEY in secrets)
    api_key = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY"))
    if api_key:
        st.session_state.bot = MusicDataBot(api_key)

# Add AI Chat Section (add this after your main dashboard content)
st.divider()
st.subheader("🤖 AI Music Insights Assistant")

if 'bot' in st.session_state:
    # Suggested questions
    col1, col2 = st.columns([2, 1])
    
    with col1:
        user_input = st.text_input("Ask about your music data:", 
                                 placeholder="e.g., What trends do you see in the current data?")
    
    with col2:
        if st.button("🎯 Get Insight"):
            if user_input:
                with st.spinner("Analyzing your data..."):
                    # Prepare data context for AI
                    data_context = {
                        'top_artists': csv_data['top_artists'],
                        'top_songs': csv_data['top_songs'],
                        'genre_popularity': csv_data['genre_popularity'],
                        'geographic_analysis': csv_data['geographic_analysis']
                    }
                    
                    response = st.session_state.bot.analyze_data(user_input, data_context)
                    
                    # Add to chat history
                    st.session_state.chat_history.append(("user", user_input))
                    st.session_state.chat_history.append(("bot", response))
    
    # Display chat history
    if st.session_state.chat_history:
        st.markdown("### 💬 Conversation")
        for i, (sender, msg) in enumerate(st.session_state.chat_history[-6:]):  # Show last 6 messages
            if sender == "user":
                message(msg, is_user=True, key=f"user_{i}")
            else:
                message(msg, key=f"bot_{i}")
    
    # Suggested questions
    st.markdown("### 💡 Suggested Questions")
    suggested = st.session_state.bot.get_suggested_questions()
    cols = st.columns(2)
    for i, question in enumerate(suggested[:6]):
        col = cols[i % 2]
        with col:
            if st.button(question, key=f"suggest_{i}"):
                # Trigger analysis with suggested question
                with st.spinner("Analyzing..."):
                    data_context = {
                        'top_artists': csv_data['top_artists'],
                        'top_songs': csv_data['top_songs'],
                        'genre_popularity': csv_data['genre_popularity'],
                        'geographic_analysis': csv_data['geographic_analysis']
                    }
                    response = st.session_state.bot.analyze_data(question, data_context)
                    st.session_state.chat_history.append(("user", question))
                    st.session_state.chat_history.append(("bot", response))
                    st.rerun()

else:
    st.warning("🔑 OpenAI API key not configured. Add OPENAI_API_KEY to secrets to enable AI insights.")
```

5. **Add secrets configuration** (create `.streamlit/secrets.toml`):
```toml
OPENAI_API_KEY = "your-openai-api-key-here"
```

## Option 2: Local AI with Hugging Face Transformers

For a free, local solution:

```bash
pip install transformers torch sentence-transformers
```

This would run locally but with more limited capabilities.

## Option 3: Custom Rule-Based Insights

Create smart insights without external APIs:

```python
class DataInsightBot:
    def generate_insights(self, data: Dict) -> List[str]:
        insights = []
        
        # Top artist insights
        top_artist = data['top_artists'].iloc[0]
        insights.append(f"🎤 {top_artist['artist']} is dominating with {top_artist['play_count']:,} plays!")
        
        # Geographic insights
        top_city = data['geographic_analysis'].iloc[0]
        insights.append(f"🌍 {top_city['city']}, {top_city['state']} leads in activity with {top_city['total_plays']:,} plays")
        
        # Genre trends
        top_genre = data['genre_popularity'].iloc[0]
        insights.append(f"🎵 {top_genre['genre'].title()} genre is trending with {top_genre['play_count']:,} plays")
        
        return insights
```

## Implementation Priority

1. **Start with Option 3** (rule-based) for immediate results
2. **Add Option 1** (OpenAI) for advanced insights
3. **Consider Option 2** for cost-sensitive deployments

## Benefits of AI Bot Integration

- **Real-time Analysis**: Instant insights on current data
- **Natural Language**: Users can ask questions naturally
- **Pattern Recognition**: AI can spot trends humans might miss
- **Interactive Experience**: Makes dashboard more engaging
- **Business Value**: Provides actionable recommendations

## Next Steps

1. Choose your preferred option
2. Set up API keys (if using OpenAI)
3. Install required packages
4. Add bot module to your dashboard
5. Test with sample questions
6. Deploy updated dashboard

Would you like me to help implement any of these options for your dashboard?
