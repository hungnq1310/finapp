#!/bin/bash

# Financial News Analysis API Backend Startup Script

echo "🚀 Starting Financial News Analysis Backend..."

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed"
    exit 1
fi

# Check if pip is available
if ! command -v pip &> /dev/null; then
    echo "❌ pip is required but not installed"
    exit 1
fi

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Copy environment template if .env doesn't exist
if [ ! -f .env ]; then
    echo "⚙️ Creating .env from template..."
    cp .env.example .env
    echo "⚠️ Please update .env with your actual configuration"
fi

# Start the API server
echo "🌐 Starting API server on http://0.0.0.0:8001"
echo "📚 API Documentation available at http://localhost:8001/docs"
echo "💚 Health check available at http://localhost:8001/health"

python src/api_backend.py
