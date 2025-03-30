#!/bin/bash
# Cross-platform installation script for Data Analysis GUI
# Works on macOS, Linux, and Windows (with Git Bash or WSL)

# Terminal colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Data Analysis GUI - Installation Script    ${NC}"
echo -e "${BLUE}=============================================${NC}"

# --- Prerequisite Checks ---
echo -e "\n${YELLOW}Checking prerequisites...${NC}"

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check Python
if command_exists python3; then
    PYTHON_CMD="python3"
elif command_exists python; then
    PYTHON_CMD="python"
else
    echo -e "${RED}Error: Python 3 is not installed or not in PATH.${NC}"
    echo -e "${YELLOW}Please install Python 3.8+ and ensure it's added to your system PATH.${NC}"
    exit 1
fi
PY_VERSION=$($PYTHON_CMD -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo -e "${GREEN}✓ Found Python $($PYTHON_CMD --version)${NC}"
# Basic version check (e.g., >= 3.8) - adapt if specific features need higher
if [[ "$(printf '%s\n' "3.8" "$PY_VERSION" | sort -V | head -n1)" != "3.8" ]]; then
     echo -e "${RED}Error: Python 3.8 or higher is required. Found ${PY_VERSION}.${NC}"
     exit 1
fi

# Check Node.js
if ! command_exists node; then
    echo -e "${RED}Error: Node.js is not installed or not in PATH.${NC}"
    echo -e "${YELLOW}Please install Node.js (LTS version recommended, e.g., 18+) and ensure it's added to your system PATH.${NC}"
    exit 1
fi
NODE_VERSION=$(node --version)
echo -e "${GREEN}✓ Found Node.js ${NODE_VERSION}${NC}"

# Check npm
if ! command_exists npm; then
    echo -e "${RED}Error: npm is not installed or not in PATH.${NC}"
    echo -e "${YELLOW}npm usually comes with Node.js. Please check your Node.js installation.${NC}"
    exit 1
fi
NPM_VERSION=$(npm --version)
echo -e "${GREEN}✓ Found npm ${NPM_VERSION}${NC}"
echo -e "${GREEN}✓ Prerequisites met.${NC}"

# --- Create Project Structure ---
echo -e "\n${YELLOW}Ensuring project directories exist...${NC}"
mkdir -p backend/app/services
mkdir -p frontend/public
mkdir -p frontend/src/components
mkdir -p frontend/src/services
mkdir -p frontend/src/styles
echo -e "${GREEN}✓ Project directories checked/created.${NC}"

# --- Backend Setup ---
echo -e "\n${YELLOW}Setting up backend...${NC}"
cd backend || { echo -e "${RED}Error: Failed to navigate to backend directory.${NC}"; exit 1; }

# Create/Check Virtual Environment
VENV_DIR="venv"
if [ ! -d "$VENV_DIR" ]; then
    echo -e "${YELLOW}Creating Python virtual environment ('$VENV_DIR')...${NC}"
    if ! $PYTHON_CMD -m venv "$VENV_DIR"; then
        echo -e "${RED}Error: Failed to create Python virtual environment.${NC}"
        echo -e "${YELLOW}Make sure you have the necessary permissions and the 'venv' module is available.${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Virtual environment created.${NC}"
else
    echo -e "${GREEN}✓ Virtual environment '$VENV_DIR' already exists.${NC}"
fi

# Activate and Install Dependencies
echo -e "${YELLOW}Installing backend dependencies from requirements.txt...${NC}"
# Activate based on OS
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then # Git Bash on Windows
    source "$VENV_DIR/Scripts/activate"
elif [[ "$OSTYPE" == "win32" || "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then # Other Windows (might need cmd activation if run outside bash)
     echo -e "${YELLOW}Note: On Windows CMD/PowerShell, activate manually: .\\${VENV_DIR}\\Scripts\\activate${NC}"
     # Attempt Git Bash style activation anyway, might work
     source "$VENV_DIR/Scripts/activate" || echo -e "${YELLOW}Activation might require manual step.${NC}"
else # Linux/macOS
    source "$VENV_DIR/bin/activate"
fi

# Upgrade pip first
python -m pip install --upgrade pip
# Install from requirements file
if [ -f "requirements.txt" ]; then
    python -m pip install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo -e "${RED}Error: Failed to install backend dependencies.${NC}"
        # Deactivate on error before exiting
        deactivate > /dev/null 2>&1 || true
        exit 1
    fi
else
    echo -e "${RED}Error: backend/requirements.txt not found!${NC}"
    deactivate > /dev/null 2>&1 || true
    exit 1
fi

# Deactivate environment (optional, good practice in scripts)
deactivate > /dev/null 2>&1 || true

echo -e "${GREEN}✓ Backend setup complete.${NC}"
cd .. || exit 1 # Go back to root

# --- Frontend Setup ---
echo -e "\n${YELLOW}Setting up frontend...${NC}"
cd frontend || { echo -e "${RED}Error: Failed to navigate to frontend directory.${NC}"; exit 1; }

# Check/Create Config Files
# .babelrc (optional if react-scripts handles it, but good for explicit config)
if [ ! -f ".babelrc" ]; then
  echo -e "${YELLOW}Creating .babelrc...${NC}"
  cat > .babelrc << EOF
{
  "presets": [
    "@babel/preset-env",
    ["@babel/preset-react", { "runtime": "automatic" }]
  ]
}
EOF
fi

# postcss.config.js
if [ ! -f "postcss.config.js" ]; then
  echo -e "${YELLOW}Creating postcss.config.js...${NC}"
  cat > postcss.config.js << EOF
module.exports = {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
  }
}
EOF
fi

# tailwind.config.js
if [ ! -f "tailwind.config.js" ]; then
  echo -e "${YELLOW}Creating tailwind.config.js...${NC}"
  cat > tailwind.config.js << EOF
/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}", // Ensure this covers your component files
    "./public/index.html"
  ],
  theme: {
    extend: {},
  },
  plugins: [],
}
EOF
fi

# src/styles/index.css (ensure Tailwind directives)
INDEX_CSS="src/styles/index.css"
if [ ! -f "$INDEX_CSS" ]; then
    echo -e "${YELLOW}Creating $INDEX_CSS...${NC}"
    mkdir -p src/styles
fi
# Ensure Tailwind directives are present
if ! grep -q "@tailwind base;" "$INDEX_CSS"; then
    echo -e "${YELLOW}Adding Tailwind directives to $INDEX_CSS...${NC}"
    cat > "$INDEX_CSS" << EOF
@tailwind base;
@tailwind components;
@tailwind utilities;

body {
  margin: 0;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen',
    'Ubuntu', 'Cantarell', 'Fira Sans', 'Droid Sans', 'Helvetica Neue',
    sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}

code {
  font-family: source-code-pro, Menlo, Monaco, Consolas, 'Courier New',
    monospace;
}
EOF
fi

# Install Frontend Dependencies
if [ -f "package.json" ]; then
    echo -e "${YELLOW}Installing frontend dependencies from package.json...${NC}"
    # Use --legacy-peer-deps if needed, but try without first
    npm install
    if [ $? -ne 0 ]; then
        echo -e "${YELLOW}npm install failed. Trying with --legacy-peer-deps...${NC}"
        npm install --legacy-peer-deps
        if [ $? -ne 0 ]; then
            echo -e "${RED}Error: Failed to install frontend dependencies even with --legacy-peer-deps.${NC}"
            echo -e "${YELLOW}Check package.json, network connection, and npm logs.${NC}"
            exit 1
        fi
    fi
else
    echo -e "${RED}Error: frontend/package.json not found!${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Frontend setup complete.${NC}"
cd .. || exit 1 # Go back to root

# --- Create Startup Scripts ---
echo -e "\n${YELLOW}Creating startup scripts (start.sh, start.bat)...${NC}"

# Create start.sh (macOS/Linux/Git Bash)
cat > start.sh << 'EOF'
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

EOF

# Create start.bat (Windows CMD/PowerShell)
cat > start.bat << 'EOF'
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
EOF

# Make start.sh executable
chmod +x start.sh

echo -e "${GREEN}✓ Startup scripts created.${NC}"

# --- Final Instructions ---
echo -e "\n${GREEN}==================================================${NC}"
echo -e "${GREEN}✓ Installation complete!${NC}"
echo -e "${GREEN}==================================================${NC}"
echo -e "${YELLOW}To start the application:${NC}"
echo -e "  ${BLUE}macOS/Linux/Git Bash:${NC} ./start.sh"
echo -e "  ${BLUE}Windows (CMD/PowerShell):${NC} .\start.bat"
echo -e "\n${YELLOW}Default Ports:${NC}"
echo -e "  Backend API: ${BLUE}8000${NC}"
echo -e "  Frontend UI: ${BLUE}3000${NC}"
echo -e "\n${YELLOW}To use different ports (if defaults are taken):${NC}"
echo -e "  Set environment variables ${RED}BEFORE${NC} running the start script:"
echo -e "  ${CYAN}Example (macOS/Linux/Git Bash):${NC}"
echo -e "    export BACKEND_PORT=8001"
echo -e "    export PORT=3001"
echo -e "    ./start.sh"
echo -e "  ${CYAN}Example (Windows CMD):${NC}"
echo -e "    set BACKEND_PORT=8001"
echo -e "    set PORT=3001"
echo -e "    .\start.bat"
echo -e "  ${CYAN}Example (Windows PowerShell):${NC}"
echo -e "    \$env:BACKEND_PORT = '8001'"
echo -e "    \$env:PORT = '3001'"
echo -e "    .\start.bat"
echo -e "\n  ${RED}IMPORTANT:${NC} If you change ${YELLOW}BACKEND_PORT${NC}, you MUST manually update the"
echo -e "  'proxy' setting in ${BLUE}frontend/package.json${NC} to match the new backend port"
echo -e "  (e.g., \"proxy\": \"http://localhost:8001\") and then restart the frontend"
echo -e "  (stop the script/windows and run start.sh/start.bat again)."
echo -e "\n${YELLOW}The application will:${NC}"
echo -e "  1. Start the backend server on the specified/default port."
echo -e "  2. Start the frontend server on the specified/default port."
echo -e "  3. Attempt to open the frontend URL in your web browser."
echo -e "\n${YELLOW}To stop the application:${NC}"
echo -e "  - If using ${BLUE}start.sh${NC}: Press ${RED}Ctrl+C${NC} in the terminal where you ran the script."
echo -e "  - If using ${BLUE}start.bat${NC}: Close the ${RED}'Backend Server'${NC} and ${RED}'Frontend Server'${NC} command prompt windows."
echo -e "${GREEN}==================================================${NC}"

# Optional: Ask to start now
read -p "Would you like to try starting the application now? (y/n) " -n 1 -r
echo # Move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "\n${GREEN}Attempting to start application...${NC}"
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "linux-gnu"* || "$OSTYPE" == "darwin"* ]]; then
        ./start.sh
    elif [[ "$OSTYPE" == "win32" ]]; then
        start cmd /c start.bat # Use start cmd /c to launch the bat in a truly new process
    else
         echo -e "${YELLOW}Cannot determine OS type to auto-start. Please use start.sh or start.bat manually.${NC}"
    fi
fi