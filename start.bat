@echo off
echo Starting Data Analysis GUI...

:: Start backend server
start cmd /k "cd backend\app && ..\venv\Scripts\activate && uvicorn main:app --reload"

:: Wait for backend to start
timeout /t 5 /nobreak

:: Start frontend server
start cmd /k "cd frontend && npm start"

:: Open browser after a short delay
timeout /t 5 /nobreak
start http://localhost:3000

echo Data Analysis GUI is now running!
echo Close the command windows to stop the servers when done.
