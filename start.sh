#!/bin/bash
# Cross-platform startup script for Data Analysis GUI

# Terminal colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Starting Data Analysis GUI               ${NC}"
echo -e "${BLUE}=============================================${NC}"

# Function to clean up background processes on exit
cleanup() {
    echo -e "\n${YELLOW}Shutting down servers...${NC}"
    # Kill processes using their PIDs
    if [ -n "$BACKEND_PID" ]; then
        kill "$BACKEND_PID" > /dev/null 2>&1
        wait "$BACKEND_PID" > /dev/null 2>&1 # Wait for it to actually terminate
    fi
     if [ -n "$FRONTEND_PID" ]; then
        # npm start often spawns child processes, try killing the group
        kill -TERM -- "-$FRONTEND_PID" > /dev/null 2>&1 || kill "$FRONTEND_PID" > /dev/null 2>&1
        wait "$FRONTEND_PID" > /dev/null 2>&1
    fi
    echo -e "${GREEN}Servers stopped.${NC}"
    exit 0
}

# Trap SIGINT (Ctrl+C) and SIGTERM to run cleanup
trap cleanup SIGINT SIGTERM

# Start backend server
echo -e "${YELLOW}Starting backend server...${NC}"
cd backend || { echo -e "${RED}Error: Failed to navigate to backend directory${NC}"; exit 1; }

# Activate venv and run uvicorn *from the backend directory*
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then # Git Bash/WSL/etc.
    source venv/Scripts/activate
else # Linux/macOS
    source venv/bin/activate
fi
if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Failed to activate backend virtual environment.${NC}"
    exit 1
fi

# Start Uvicorn in the background
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload &
BACKEND_PID=$!
echo -e "${GREEN}✓ Backend server starting (PID: $BACKEND_PID) on port 8000...${NC}"
# Deactivate after starting (uvicorn runs in its own process)
deactivate > /dev/null 2>&1 || true
cd .. || exit 1 # Back to root

# Wait a few seconds for backend
echo -e "${YELLOW}Waiting for backend (5s)...${NC}"
sleep 5

# Start frontend server
echo -e "${YELLOW}Starting frontend server...${NC}"
cd frontend || { echo -e "${RED}Error: Failed to navigate to frontend directory${NC}"; exit 1; }

# Start React app in the background (setsid ensures child processes are grouped)
setsid npm start &
FRONTEND_PID=$!
echo -e "${GREEN}✓ Frontend server starting (PID: $FRONTEND_PID) on port 3000...${NC}"
cd .. || exit 1 # Back to root

# Wait a few seconds for frontend
echo -e "${YELLOW}Waiting for frontend (5s)...${NC}"
sleep 5

# Open browser
URL="http://localhost:3000"
echo -e "${YELLOW}Attempting to open $URL in your browser...${NC}"
if [[ "$OSTYPE" == "darwin"* ]]; then
    open "$URL"
elif [[ "$OSTYPE" == "linux-gnu"* || "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    xdg-open "$URL" > /dev/null 2>&1 || echo -e "${YELLOW}Please open $URL manually.${NC}"
else # Fallback for other systems or if xdg-open fails
    echo -e "${YELLOW}Cannot automatically open browser. Please open $URL manually.${NC}"
fi

echo -e "\n${GREEN}✓ Data Analysis GUI should be running!${NC}"
echo -e "${YELLOW}Backend logs are in the terminal where backend started (or check uvicorn output)."${NC}
echo -e "${YELLOW}Frontend logs are in the terminal where frontend started."${NC}
echo -e "${YELLOW}Press Ctrl+C in this terminal to stop both servers.${NC}\n"

# Keep the script running to manage background processes
wait $BACKEND_PID
wait $FRONTEND_PID
# If wait returns immediately (process died), call cleanup
cleanup

