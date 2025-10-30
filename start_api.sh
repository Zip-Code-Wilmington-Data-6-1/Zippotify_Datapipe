#!/bin/bash
# FastAPI Server Launcher

echo "🚀 Starting FastAPI Server..."
cd /Users/sai/Documents/Projects/final-project/Zippotify_Datapipe

# Check if fast_api.py exists
if [ ! -f "fast_api.py" ]; then
    echo "❌ fast_api.py not found in current directory"
    exit 1
fi

# Start server
echo "📡 Starting server on http://localhost:8001"
echo "🔗 API Documentation: http://localhost:8001/docs"
echo "🛑 Press Ctrl+C to stop"

uvicorn fast_api:app --host 127.0.0.1 --port 8001 --reload