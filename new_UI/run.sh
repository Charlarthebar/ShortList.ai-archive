#!/bin/bash
# Run ShortList New UI
# Usage: ./run.sh

set -e

cd "$(dirname "$0")"

echo "ShortList - New UI"
echo "=================="
echo ""

# Check if we should run schema updates
if [ "$1" == "--setup" ]; then
    echo "Running database schema updates..."
    psql -d jobs_db -f backend/schema.sql
    echo "Schema updated."
    echo ""
fi

# Start backend
echo "Starting backend API on http://localhost:5002 ..."
cd backend
python3 app.py &
BACKEND_PID=$!
cd ..

# Wait for backend to start
sleep 2

# Start frontend (simple Python HTTP server)
echo "Starting frontend on http://localhost:8000 ..."
cd frontend
python3 -m http.server 8000 &
FRONTEND_PID=$!
cd ..

echo ""
echo "Services running:"
echo "  - Backend API: http://localhost:5002"
echo "  - Frontend:    http://localhost:8000"
echo ""
echo "Press Ctrl+C to stop both services."

# Trap Ctrl+C and cleanup
trap "echo 'Stopping services...'; kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit" SIGINT SIGTERM

# Wait for processes
wait
