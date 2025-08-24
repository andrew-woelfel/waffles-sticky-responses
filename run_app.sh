#!/bin/bash

# Quick start script for Help Scout Analytics Project

echo "🚀 Starting Help Scout Analytics Application..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please run setup_project.sh first."
    exit 1
fi

# Activate virtual environment
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "⚠️  .env file not found. Creating from template..."
    cp .env.example .env
    echo "📝 Please edit .env file and add your OpenAI API key before continuing."
    echo "   Then run this script again."
    exit 1
fi

# Check if data file exists, if not inform user
if [ ! -f "data/saas_customer_data.csv" ]; then
    echo "📊 No data file found. The app will generate sample data for development."
    echo "   To use real data, place saas_customer_data.csv in the data/ directory."
fi

echo "🌐 Starting Streamlit application..."
echo "   App will be available at: http://localhost:8501"
echo ""
echo "   Press Ctrl+C to stop the application"
echo ""

# Run Streamlit app
streamlit run streamlit_app.py