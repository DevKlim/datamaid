#!/bin/bash
# Cross-platform startup script for Data Analysis GUI (macOS/Linux focus)

# Terminal colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Starting Data Analysis GUI               ${NC}"
echo -e "${BLUE}=============================================${NC}"

# Store PIDs for cleanup
BACKEND_PID=""
FRONTEND_PID=""

# Function to clean up background processes on exit
cleanup() {
    echo -e "\n${YELLOW}Shutting down servers...${NC}"
    # Kill processes using their PIDs
    if [ -n "$FRONTEND_PID" ]; then
        echo "Stopping frontend (PID $FRONTEND_PID)..."
        # Try killing the process group first, then the specific PID
        kill -TERM -- "-$FRONTEND_PID" > /dev/null 2>&1 || kill "$FRONTEND_PID" > /dev/null 2>&1
        wait "$FRONTEND_PID" 2>/dev/null
    fi
    if [ -n "$BACKEND_PID" ]; then
        echo "Stopping backend (PID $BACKEND_PID)..."
        kill "$BACKEND_PID" > /dev/null 2>&1
        wait "$BACKEND_PID" 2>/dev/null # Wait for it to actually terminate
    fi
    echo -e "${GREEN}Servers stopped.${NC}"
    exit 0
}

# Trap SIGINT (Ctrl+C) and SIGTERM to run cleanup
trap cleanup SIGINT SIGTERM

# --- Backend Setup ---
echo -e "${YELLOW}Starting backend server...${NC}"
cd backend || { echo -e "${RED}Error: Failed to navigate to backend directory${NC}"; exit 1; }

# Check for venv
if [ ! -f "venv/bin/activate" ]; then
    echo -e "${RED}Error: Backend virtual environment not found in ./backend/venv/${NC}"
    echo -e "${YELLOW}Please run the backend setup/installation steps first.${NC}"
    cd ..
    exit 1
fi

# Activate venv
echo "Activating backend venv..."
source venv/bin/activate
if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Failed to activate backend virtual environment.${NC}"
    cd ..
    exit 1
fi

# Start Uvicorn in the background
echo "Starting Uvicorn..."
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload &
BACKEND_PID=$!
if [ -z "$BACKEND_PID" ]; then
    echo -e "${RED}Error: Failed to start backend server.${NC}"
    deactivate > /dev/null 2>&1 || true
    cd ..
    exit 1
fi
echo -e "${GREEN}✓ Backend server starting (PID: $BACKEND_PID) on port 8000...${NC}"

# Deactivate venv (the background process keeps using it)
deactivate > /dev/null 2>&1 || true
cd .. || { echo -e "${RED}Error: Failed to navigate back to root directory${NC}"; exit 1; } # Back to root

# --- Wait for Backend ---
echo -e "${YELLOW}Waiting for backend (5s)...${NC}"
sleep 5

# --- Frontend Setup ---
echo -e "${YELLOW}Starting frontend server...${NC}"
cd frontend || { echo -e "${RED}Error: Failed to navigate to frontend directory${NC}"; cleanup; exit 1; }

# Check for node_modules
if [ ! -d "node_modules" ]; then
    echo -e "${RED}Error: Frontend dependencies not found in ./frontend/node_modules/${NC}"
    echo -e "${YELLOW}Please run 'npm install' in the frontend directory first.${NC}"
    cd ..
    cleanup # Stop backend if frontend fails
    exit 1
fi

# --- CRITICAL DEBUG STEP for allowedHosts ---
# Before running npm start, let's check relevant environment variables
echo -e "${YELLOW}Checking environment variables potentially affecting Dev Server...${NC}"
echo "HOST=$HOST"
echo "ALLOWED_HOSTS=$ALLOWED_HOSTS"
echo "DANGEROUSLY_DISABLE_HOST_CHECK=$DANGEROUSLY_DISABLE_HOST_CHECK"
echo "WDS_SOCKET_HOST=$WDS_SOCKET_HOST"
echo "WDS_SOCKET_PORT=$WDS_SOCKET_PORT"
echo "PUBLIC_URL=$PUBLIC_URL"
echo -e "${YELLOW}----------------------------------------------------------${NC}"
# --- End Debug Step ---

# Start React app in the background
echo "Starting React app (npm start)..."
# Using setsid to attempt better process group management if available
npm start &> npm_start.log &
FRONTEND_PID=$!

# Give npm start a moment to potentially fail early
sleep 2

# Check if npm start process is still running
if ! ps -p $FRONTEND_PID > /dev/null; then
    echo -e "${RED}Error: Frontend server (npm start) failed to start or exited quickly.${NC}"
    echo -e "${YELLOW}Check the 'frontend/npm_start.log' file for details.${NC}"
    cd ..
    cleanup # Stop backend
    exit 1
fi

echo -e "${GREEN}✓ Frontend server starting (PID: $FRONTEND_PID) on port 3000...${NC}"
echo -e "${YELLOW}Frontend logs are being saved to 'frontend/npm_start.log'. Tail it in another terminal if needed: tail -f frontend/npm_start.log${NC}"
cd .. || { echo -e "${RED}Error: Failed to navigate back to root directory${NC}"; cleanup; exit 1; } # Back to root

# --- Wait for Frontend ---
echo -e "${YELLOW}Waiting for frontend (5s)...${NC}"
sleep 5

# --- Open Browser ---
URL="http://localhost:3000"
echo -e "${YELLOW}Attempting to open $URL in your browser...${NC}"
# macOS specific command
open "$URL" || echo -e "${YELLOW}Failed to automatically open browser. Please open $URL manually.${NC}"


echo -e "\n${GREEN}✓ Data Analysis GUI should be running!${NC}"
echo -e "${YELLOW}Backend logs are visible if you started it manually or check Uvicorn output."${NC}
echo -e "${YELLOW}Frontend logs are in 'frontend/npm_start.log'.${NC}"
echo -e "${YELLOW}Press Ctrl+C in this terminal to stop both servers.${NC}\n"

# Keep the script running to manage background processes
# Wait for the *frontend* first, as it's often the primary interface.
# If it dies, we likely want to shut down the backend too.
wait $FRONTEND_PID
echo -e "${YELLOW}Frontend process ended. Shutting down...${NC}"
cleanup # Call cleanup if frontend stops

# Fallback wait in case frontend wait fails or backend needs waiting too
wait $BACKEND_PID
cleanup # Call cleanup if backend stops

exit 0 # Should be handled by cleanup, but good practice