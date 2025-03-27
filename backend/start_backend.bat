@echo off
echo Starting Data Analysis GUI Backend...

REM Activate virtual environment if it exists
if exist venv\Scripts\activate (
    call venv\Scripts\activate
) else (
    echo Virtual environment not found. Please run setup script first.
    exit /b 1
)

REM Change to app directory
cd app

REM Start the FastAPI server
echo Starting FastAPI server...
uvicorn main:app --reload

REM Deactivate virtual environment when server stops
call deactivate