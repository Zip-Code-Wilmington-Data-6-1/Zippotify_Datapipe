#!/bin/bash

# Zip-potify Dashboard Launcher
# Phase 1 - Static Dashboard

echo "🎧 Starting Zip-potify Analytics Dashboard..."
echo "📊 Phase 1 - Static Dashboard with comprehensive analytics"
echo ""

# Check if we're in the right directory
if [ ! -f "dashboard.py" ]; then
    echo "❌ Error: dashboard.py not found. Please run from the static-dashboard directory."
    exit 1
fi

# Check if data files exist
if [ ! -f "../aggregated_music_data.json" ]; then
    echo "⚠️  Warning: aggregated_music_data.json not found."
    echo "📝 Please run the ETL pipeline first:"
    echo "   cd .. && python etl/aggregated_data.py"
    echo ""
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Install requirements if needed
if ! python -c "import streamlit, plotly, pandas" 2>/dev/null; then
    echo "📦 Installing dashboard requirements..."
    pip install -r requirements.txt
fi

echo ""
echo "🚀 Launching dashboard on http://localhost:8501"
echo "🛑 Press Ctrl+C to stop the dashboard"
echo ""

# Launch Streamlit dashboard
streamlit run dashboard.py --server.port 8501 --server.address localhost
