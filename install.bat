@echo off
echo Starting Data Analysis GUI installation...

REM Check if Python is installed
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Python is not installed or not in PATH. Please install Python 3.9+ first.
    exit /b 1
)

REM Check if Node.js is installed
node --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Node.js is not installed or not in PATH. Please install Node.js 18+ first.
    exit /b 1
)

REM Create project directories if they don't exist
if not exist backend\app mkdir backend\app
if not exist frontend\src\components mkdir frontend\src\components
if not exist frontend\src\services mkdir frontend\src\services
if not exist frontend\src\styles mkdir frontend\src\styles
if not exist frontend\public mkdir frontend\public

echo Setting up backend...
cd backend

REM Create Python virtual environment
python -m venv venv

REM Activate virtual environment
call venv\Scripts\activate

REM Install backend dependencies
pip install fastapi uvicorn python-multipart pandas polars duckdb pydantic numpy

REM Save dependencies to requirements.txt
pip freeze > requirements.txt

echo Backend setup complete!

REM Go back to project root
cd ..

echo Setting up frontend...
cd frontend

REM Initialize npm project (non-interactive)
call npm init -y

REM Install frontend dependencies
call npm install react react-dom react-scripts @headlessui/react @heroicons/react axios chart.js react-chartjs-2 react-syntax-highlighter tailwindcss@latest postcss autoprefixer

REM Initialize tailwind
call npx tailwindcss init -p

echo Frontend setup complete!

REM Go back to project root
cd ..

echo Installation complete! Now run the application with:
echo python run.py

pause