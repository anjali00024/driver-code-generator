#!/bin/bash

# Start the backend server
echo "Starting backend server..."
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000 &
BACKEND_PID=$!
echo "Backend started with PID: $BACKEND_PID"

# Start the frontend server
echo "Starting frontend server..."
cd ../frontend
npm install
npm start &
FRONTEND_PID=$!
echo "Frontend started with PID: $FRONTEND_PID"

echo "Both servers are now running."
echo "Backend: http://localhost:8000"
echo "Frontend: http://localhost:3000"
echo ""
echo "Press Ctrl+C to stop both servers."

# Wait for user to press Ctrl+C
wait $BACKEND_PID $FRONTEND_PID 