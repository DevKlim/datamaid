#!/bin/bash
# Cross-platform startup script for Data Analysis GUI

# Terminal colors for better readability
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}=============================================\033[0m"
echo -e "${BLUE}  Starting Data Analysis GUI               \033[0m"
echo -e "${BLUE}=============================================\033[0m"

# Detect operating system
if [[ "$OSTYPE" == "darwin"* ]]; then
    OS="macOS"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS="Linux"
elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
    OS="Windows"
else
    OS="Unknown"
fi

echo -e "${YELLOW}Detected operating system: ${OS}\033[0m"

# Start backend server
echo -e "${YELLOW}Starting backend server...\033[0m"
cd backend || { echo -e "${RED}Error: Failed to navigate to backend directory\033[0m"; exit 1; }

# Activate virtual environment and start server
if [[ "$OS" == "Windows" && "$OSTYPE" != "msys" && "$OSTYPE" != "cygwin" ]]; then
    # For Windows (non-Bash shell)
    start cmd /k "cd app && ..\venv\Scripts\activate && uvicorn main:app --reload"
else
    # For macOS, Linux, Git Bash, WSL
    cd app
    # Run in the background and capture the process ID
    source ../venv/bin/activate && uvicorn main:app --reload &
    BACKEND_PID=$!
    cd ..
fi

# Return to project root
cd ..

# Wait for backend to start
echo -e "${YELLOW}Waiting for backend to initialize (5 seconds)...\033[0m"
sleep 5

# Start frontend server
echo -e "${YELLOW}Starting frontend server...\033[0m"
cd frontend || { echo -e "${RED}Error: Failed to navigate to frontend directory\033[0m"; exit 1; }

if [[ "$OS" == "Windows" && "$OSTYPE" != "msys" && "$OSTYPE" != "cygwin" ]]; then
    # For Windows (non-Bash shell)
    start cmd /k "npm start"
else
    # For macOS, Linux, Git Bash, WSL
    npm start &
    FRONTEND_PID=$!
fi

# Return to project root
cd ..

# Open browser after a short delay
echo -e "${YELLOW}Opening browser in 5 seconds...\033[0m"
sleep 5

if [[ "$OS" == "Windows" && "$OSTYPE" != "msys" && "$OSTYPE" != "cygwin" ]]; then
    # For Windows (non-Bash shell)
    start http://localhost:3000
elif [[ "$OS" == "macOS" ]]; then
    # For macOS
    open http://localhost:3000
else
    # For Linux
    xdg-open http://localhost:3000 2>/dev/null || sensible-browser http://localhost:3000 2>/dev/null || open http://localhost:3000 2>/dev/null || echo -e "${YELLOW}Please open http://localhost:3000 in your browser\033[0m"
fi

echo -e "${GREEN}✓ Data Analysis GUI is now running!\033[0m"
echo -e "${YELLOW}Press Ctrl+C to stop the servers\033[0m"

# Wait for user to press Ctrl+C
trap "echo -e '${YELLOW}Shutting down servers...\033[0m'; if [[ \"$OS\" != \"Windows\" || \"$OSTYPE\" == \"msys\" || \"$OSTYPE\" == \"cygwin\" ]]; then kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; fi; echo -e '${GREEN}Servers stopped\033[0m'" INT

# Keep script running until Ctrl+C is pressed
if [[ "$OS" != "Windows" || "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    wait
fi
