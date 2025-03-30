@echo off
echo Starting Data Analysis GUI...
setlocal

:: Store current directory
set START_DIR=%CD%

:: Start backend server
echo Starting backend server...
cd backend
if not exist "venv\Scripts\activate.bat" (
    echo ERROR: Backend virtual environment not found in .\backend\venv\
    echo Please run install.sh first.
    goto :eof
)
:: Start in a new window, activate venv, run uvicorn from backend dir
start "Backend Server" cmd /k (echo Activating backend venv... ^& call venv\Scripts\activate.bat ^& echo Starting Uvicorn... ^& uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload)
cd %START_DIR%

:: Wait a bit for the backend server
echo Waiting for backend (5s)...
timeout /t 5 /nobreak > nul

:: Start frontend server
echo Starting frontend server...
cd frontend
if not exist "node_modules" (
    echo ERROR: Frontend dependencies not found in .\frontend\node_modules\
    echo Please run install.sh first.
    goto :eof
)
:: Start in a new window
start "Frontend Server" cmd /k (echo Starting React app... ^& npm start)
cd %START_DIR%

:: Wait a bit for the frontend server
echo Waiting for frontend (5s)...
timeout /t 5 /nobreak > nul

:: Open browser
echo Opening browser at http://localhost:3000
start "" "http://localhost:3000"

echo.
echo Data Analysis GUI backend and frontend are starting!
echo - Backend running in the "Backend Server" window (Port 8000).
echo - Frontend running in the "Frontend Server" window (Port 3000).
echo Close the command windows manually to stop the servers when done.
echo.

endlocal
