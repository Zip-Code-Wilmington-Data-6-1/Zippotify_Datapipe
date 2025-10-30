#!/bin/bash
# API-Enhanced Streamlit Dashboard Launcher

echo "🎧 Starting TracktionAI API-Enhanced Dashboard..."

# Check if we're in the right directory
if [ ! -f "api_dashboard.py" ]; then
    echo "❌ Error: api_dashboard.py not found. Please run from the static-dashboard directory."
    exit 1
fi

# Install API requirements if needed
if ! python -c "import requests" 2>/dev/null; then
    echo "📦 Installing API requirements..."
    pip install requests httpx
fi

# Check FastAPI server connection
echo "🔍 Checking FastAPI server connection..."
if curl -s http://localhost:8001/health > /dev/null 2>&1; then
    echo "✅ FastAPI server is running on port 8001"
else
    echo "⚠️  FastAPI server not detected on port 8001"
    echo "📝 To start FastAPI server:"
    echo "   cd .. && ./start_api.sh"
    echo ""
    read -p "Continue with dashboard anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""
echo "🚀 Launching API-Enhanced Dashboard on http://localhost:8501"
echo "🛑 Press Ctrl+C to stop the dashboard"
echo ""

# Launch Streamlit dashboard
streamlit run api_dashboard.py --server.port 8501 --server.address localhost