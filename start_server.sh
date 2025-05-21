#!/bin/bash

# Kill any existing uvicorn processes
pkill -f uvicorn || true

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Initialize pyenv
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

# Set Python version and create/activate virtual environment if it doesn't exist
pyenv local 3.7.13
if [ ! -d "venv" ]; then
    python -m venv venv
fi
source venv/bin/activate

# Upgrade pip and install dependencies
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# Try port 8000 first, fallback to 8001 if busy
PORT=8000
if lsof -i:8000 > /dev/null 2>&1; then
    PORT=8001
    echo "Port 8000 is in use, using port 8001 instead"
fi

echo "Starting server on port $PORT..."

# Change to backend directory and start the server
cd backend
exec python -m uvicorn main:app --reload --host 0.0.0.0 --port $PORT 