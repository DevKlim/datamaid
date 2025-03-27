@echo off
echo Starting Data Analysis GUI...

REM Start backend server
start cmd /k "cd backend\app && ..\venv\Scripts\activate && uvicorn main:app --reload"

REM Start frontend server (wait a bit for backend to initialize)
timeout /t 3
start cmd /k "cd frontend && npm start"

REM Open browser after a short delay
timeout /t 5
start http://localhost:3000

echo Both servers are now running in separate windows.
echo Press Ctrl+C in each window to stop the servers when done.