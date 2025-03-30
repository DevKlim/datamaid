#!/bin/bash
# Cross-platform startup script for Data Analysis GUI

# --- Configuration ---
DEFAULT_BACKEND_PORT=8000
DEFAULT_FRONTEND_PORT=3000

# Read ports from environment variables, use defaults if not set
ACTUAL_BACKEND_PORT=${BACKEND_PORT:-$DEFAULT_BACKEND_PORT}
ACTUAL_FRONTEND_PORT=${PORT:-$DEFAULT_FRONTEND_PORT} # React uses 'PORT'

# Terminal colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Starting Data Analysis GUI               ${NC}"
echo -e "${BLUE}=============================================${NC}"
echo -e "${YELLOW}Using Ports - Backend: ${ACTUAL_BACKEND_PORT}, Frontend: ${ACTUAL_FRONTEND_PORT}${NC}"
if [ "$ACTUAL_BACKEND_PORT" != "$DEFAULT_BACKEND_PORT" ]; then
    echo -e "${YELLOW}Warning: Backend port changed. Ensure 'proxy' in frontend/package.json matches (currently set for port ${DEFAULT_BACKEND_PORT}).${NC}"
    echo -e "${YELLOW}You may need to update it to 'http://localhost:${ACTUAL_BACKEND_PORT}' and restart the frontend.${NC}"
fi


# Function to clean up background processes on exit
cleanup() {
    echo -e "\n${YELLOW}Shutting down servers...${NC}"
    # Kill processes using their PIDs
    if [ -n "$BACKEND_PID" ]; then
        echo "Stopping backend (PID: $BACKEND_PID)..."
        # Send TERM signal first, then KILL if it doesn't stop
        kill "$BACKEND_PID" 2>/dev/null && wait "$BACKEND_PID" 2>/dev/null || kill -9 "$BACKEND_PID" 2>/dev/null
    fi
     if [ -n "$FRONTEND_PID" ]; then
        echo "Stopping frontend (PID: $FRONTEND_PID and potential children)..."
        # Try killing the process group first if setsid was used (more effective)
        # Otherwise, just kill the main PID. Might leave orphans if setsid wasn't used.
        if command_exists setsid && [ "$SETSID_USED" = true ]; then
             pkill -P "$FRONTEND_PID" 2>/dev/null # Kill children first if possible
             kill -- "-$FRONTEND_PID" 2>/dev/null # Kill process group created by setsid
        fi
        # Always try killing the main PID as fallback or primary method
        kill "$FRONTEND_PID" 2>/dev/null && wait "$FRONTEND_PID" 2>/dev/null || kill -9 "$FRONTEND_PID" 2>/dev/null
    fi
    echo -e "${GREEN}Servers stopped.${NC}"
    exit 0
}

# Trap SIGINT (Ctrl+C) and SIGTERM to run cleanup
trap cleanup SIGINT SIGTERM

# Start backend server
echo -e "${YELLOW}Starting backend server on port ${ACTUAL_BACKEND_PORT}...${NC}"
cd backend || { echo -e "${RED}Error: Failed to navigate to backend directory${NC}"; exit 1; }

# Activate venv
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
    source venv/Scripts/activate
else # Linux/macOS
    source venv/bin/activate
fi
if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Failed to activate backend virtual environment.${NC}"; exit 1;
fi

# Start Uvicorn in the background
uvicorn app.main:app --host 0.0.0.0 --port "$ACTUAL_BACKEND_PORT" --reload &
BACKEND_PID=$!
echo -e "${GREEN}✓ Backend server starting (PID: $BACKEND_PID)...${NC}"
deactivate > /dev/null 2>&1 || true
cd .. || exit 1

# Wait for backend
echo -e "${YELLOW}Waiting for backend (5s)...${NC}"
sleep 5

# Start frontend server
echo -e "${YELLOW}Starting frontend server on port ${ACTUAL_FRONTEND_PORT}...${NC}"
cd frontend || { echo -e "${RED}Error: Failed to navigate to frontend directory${NC}"; exit 1; }

# Use setsid if available for better process management, otherwise just run in background
SETSID_USED=false
if command_exists setsid; then
    echo "(Using setsid for frontend process group management)"
    PORT="$ACTUAL_FRONTEND_PORT" setsid npm start &
    FRONTEND_PID=$!
    SETSID_USED=true
else
    echo "(setsid not found, running frontend directly in background - cleanup might be less effective)"
    PORT="$ACTUAL_FRONTEND_PORT" npm start &
    FRONTEND_PID=$!
fi

echo -e "${GREEN}✓ Frontend server starting (PID: $FRONTEND_PID)...${NC}"
cd .. || exit 1

# Wait for frontend
echo -e "${YELLOW}Waiting for frontend (5s)...${NC}"
sleep 5

# Open browser
URL="http://localhost:${ACTUAL_FRONTEND_PORT}"
echo -e "${YELLOW}Attempting to open $URL in your browser...${NC}"
# (Browser opening logic remains the same)
if [[ "$OSTYPE" == "darwin"* ]]; then
    open "$URL"
elif [[ "$OSTYPE" == "linux-gnu"* || "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    xdg-open "$URL" > /dev/null 2>&1 || \
    gnome-open "$URL" > /dev/null 2>&1 || \
    kde-open "$URL" > /dev/null 2>&1 || \
    echo -e "${YELLOW}Please open $URL manually.${NC}"
elif [[ "$OSTYPE" == "win32" ]]; then
    start "" "$URL" # Use start "" for URLs on Windows Git Bash
else
    echo -e "${YELLOW}Cannot automatically open browser. Please open $URL manually.${NC}"
fi


echo -e "\n${GREEN}✓ Data Analysis GUI should be running!${NC}"
echo -e "${YELLOW}Backend API: http://localhost:${ACTUAL_BACKEND_PORT}"${NC}
echo -e "${YELLOW}Frontend UI: http://localhost:${ACTUAL_FRONTEND_PORT}"${NC}
echo -e "${YELLOW}Press Ctrl+C in this terminal to stop both servers.${NC}\n"

# Keep the script running
wait $BACKEND_PID
wait $FRONTEND_PID
cleanup # Call cleanup if processes exit unexpectedly