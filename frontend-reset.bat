#!/bin/bash
# Cross-platform installation script for Data Analysis GUI
# Works on macOS, Linux, and Windows (with Git Bash or WSL)

# Terminal colors for better readability
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Print banner
echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Data Analysis GUI - Installation Script    ${NC}"
echo -e "${BLUE}=============================================${NC}"

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

echo -e "${YELLOW}Detected operating system: ${OS}${NC}"

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check if we're running with admin/sudo privileges on Windows
if [[ "$OS" == "Windows" ]]; then
    # Check if running as Administrator (Windows)
    if [[ "$OSTYPE" != "msys" && "$OSTYPE" != "cygwin" ]]; then
        echo -e "${YELLOW}Note: On Windows, you may need to run this script as Administrator${NC}"
        echo -e "${YELLOW}or use Git Bash with appropriate permissions for virtual environment creation.${NC}"
        echo -e "${YELLOW}Press Enter to continue or Ctrl+C to abort...${NC}"
        read -r
    fi
fi

# Check Python installation
echo -e "\n${YELLOW}Checking Python installation...${NC}"
if command_exists python3; then
    PYTHON_CMD="python3"
elif command_exists python; then
    PYTHON_CMD="python"
    # Check if this is Python 3
    PY_VERSION=$($PYTHON_CMD --version 2>&1)
    if [[ ! $PY_VERSION == *"Python 3"* ]]; then
        echo -e "${RED}Error: Python 3 is required but found ${PY_VERSION}${NC}"
        exit 1
    fi
else
    echo -e "${RED}Error: Python is not installed. Please install Python 3.9+ first.${NC}"
    exit 1
fi

PYTHON_VERSION=$($PYTHON_CMD --version)
echo -e "${GREEN}✓ Found ${PYTHON_VERSION}${NC}"

# Check Node.js installation
echo -e "\n${YELLOW}Checking Node.js installation...${NC}"
if ! command_exists node; then
    echo -e "${RED}Error: Node.js is not installed. Please install Node.js 18+ first.${NC}"
    exit 1
fi

NODE_VERSION=$(node --version)
echo -e "${GREEN}✓ Found Node.js ${NODE_VERSION}${NC}"

# Check npm installation
echo -e "\n${YELLOW}Checking npm installation...${NC}"
if ! command_exists npm; then
    echo -e "${RED}Error: npm is not installed. Please install npm first.${NC}"
    exit 1
fi

NPM_VERSION=$(npm --version)
echo -e "${GREEN}✓ Found npm ${NPM_VERSION}${NC}"

# Create project directories
echo -e "\n${YELLOW}Creating project directories...${NC}"
mkdir -p backend/app
mkdir -p frontend/src/components
mkdir -p frontend/src/services
mkdir -p frontend/src/styles
mkdir -p frontend/public

# Set up backend
echo -e "\n${YELLOW}Setting up backend...${NC}"
cd backend || { echo -e "${RED}Error: Failed to navigate to backend directory${NC}"; exit 1; }

# Check if virtual environment already exists
VENV_EXISTS=false
if [[ "$OS" == "Windows" ]]; then
    if [ -d "venv" ] && [ -f "venv/Scripts/activate" ]; then
        VENV_EXISTS=true
    fi
else
    if [ -d "venv" ] && [ -f "venv/bin/activate" ]; then
        VENV_EXISTS=true
    fi
fi

# Create Python virtual environment if it doesn't exist
if [ "$VENV_EXISTS" = true ]; then
    echo -e "${GREEN}✓ Virtual environment already exists. Checking packages...${NC}"
else
    echo -e "${YELLOW}Creating Python virtual environment...${NC}"
    
    # Try to create the venv, capturing any errors
    if ! $PYTHON_CMD -m venv venv 2>/tmp/venv_error.log; then
        echo -e "${RED}Error creating virtual environment:${NC}"
        cat /tmp/venv_error.log
        
        if [[ "$OS" == "Windows" ]]; then
            echo -e "${YELLOW}On Windows, you might need to:${NC}"
            echo -e "  1. Run this script as Administrator"
            echo -e "  2. Ensure Python has permission to create directories here"
            echo -e "  3. Try running: python -m pip install --upgrade virtualenv"
        else
            echo -e "${YELLOW}Try running: $PYTHON_CMD -m pip install --upgrade virtualenv${NC}"
        fi
        
        echo -e "${YELLOW}Do you want to continue without creating a virtual environment? (y/n)${NC}"
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
fi

# Set activation command based on OS
if [[ "$OS" == "Windows" ]]; then
    ACTIVATE_CMD="venv\\Scripts\\activate"
    # For Git Bash on Windows
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
        ACTIVATE_CMD="source venv/Scripts/activate"
    fi
else
    ACTIVATE_CMD="source venv/bin/activate"
fi

# Activate virtual environment and install dependencies
echo -e "${YELLOW}Installing backend dependencies...${NC}"

# Function to check if a package is installed in the venv
check_package_installed() {
    local package="$1"
    if [[ "$OS" == "Windows" && "$OSTYPE" != "msys" && "$OSTYPE" != "cygwin" ]]; then
        cmd //c "$ACTIVATE_CMD && pip show $package > nul 2>&1"
        return $?
    else
        eval "$ACTIVATE_CMD"
        pip show "$package" > /dev/null 2>&1
        local result=$?
        deactivate
        return $result
    fi
}

# Function to install Python dependencies with specific versions
install_python_dependencies() {
    echo -e "${YELLOW}Installing specific package versions to ensure compatibility...${NC}"
    
    # First, upgrade pip
    if [[ "$OS" == "Windows" && "$OSTYPE" != "msys" && "$OSTYPE" != "cygwin" ]]; then
        cmd //c "$ACTIVATE_CMD && python -m pip install --upgrade pip"
    else
        eval "$ACTIVATE_CMD"
        python -m pip install --upgrade pip
        deactivate
    fi
    
    # Install packages with specific versions
    if [[ "$OS" == "Windows" && "$OSTYPE" != "msys" && "$OSTYPE" != "cygwin" ]]; then
        # For cmd.exe on Windows
        cmd //c "$ACTIVATE_CMD && pip install fastapi==0.95.2 uvicorn==0.22.0 python-multipart==0.0.6 pandas==2.0.2 polars==0.18.9 duckdb==0.8.1 pydantic==1.10.8 typing_extensions==4.6.3 numpy==1.24.3 && pip freeze > requirements.txt"
    else
        # For macOS, Linux, Git Bash, WSL
        eval "$ACTIVATE_CMD"
        pip install fastapi==0.95.2 uvicorn==0.22.0 python-multipart==0.0.6 pandas==2.0.2 polars==0.18.9 duckdb==0.8.1 pydantic==1.10.8 typing_extensions==4.6.3 numpy==1.24.3
        pip freeze > requirements.txt
        deactivate
    fi
}

# Install packages if needed or if we need to fix the pydantic issue
if check_package_installed "fastapi" && check_package_installed "pandas" && check_package_installed "polars"; then
    echo -e "${GREEN}✓ Key backend packages already installed${NC}"
    
    # Check specifically for pydantic_core issues
    if [[ "$OS" == "Windows" && "$OSTYPE" != "msys" && "$OSTYPE" != "cygwin" ]]; then
        cmd //c "$ACTIVATE_CMD && python -c \"import pydantic_core\" > nul 2>&1"
        if [ $? -ne 0 ]; then
            echo -e "${YELLOW}Issue with pydantic_core detected. Reinstalling pydantic...${NC}"
            install_python_dependencies
        fi
    else
        eval "$ACTIVATE_CMD"
        python -c "import pydantic_core" > /dev/null 2>&1
        if [ $? -ne 0 ]; then
            echo -e "${YELLOW}Issue with pydantic_core detected. Reinstalling pydantic...${NC}"
            deactivate
            install_python_dependencies
        else
            deactivate
        fi
    fi
else
    install_python_dependencies
fi

echo -e "${GREEN}✓ Backend setup complete!${NC}"

# Return to project root
cd ..

# Set up frontend
echo -e "\n${YELLOW}Setting up frontend...${NC}"
cd frontend || { echo -e "${RED}Error: Failed to navigate to frontend directory${NC}"; exit 1; }

# Clean up existing node_modules and package-lock.json to ensure a fresh install
echo -e "${YELLOW}Cleaning up existing frontend dependencies...${NC}"
if [[ "$OS" == "Windows" ]]; then
    # For Windows
    if [ -d "node_modules" ]; then
        rm -rf node_modules
    fi
    if [ -f "package-lock.json" ]; then
        rm package-lock.json
    fi
else
    # For macOS/Linux
    rm -rf node_modules package-lock.json 2>/dev/null
fi

# Create or update package.json
if [ -f "package.json" ]; then
    echo -e "${GREEN}✓ package.json exists, updating dependencies...${NC}"
else
    echo -e "${YELLOW}Creating package.json...${NC}"
    npm init -y
fi

# Add .babelrc file to fix babel preset issues
echo -e "${YELLOW}Creating .babelrc file...${NC}"
cat > .babelrc << EOF
{
  "presets": [
    "@babel/preset-env",
    "@babel/preset-react"
  ]
}
EOF

# Create postcss.config.js with proper configuration
echo -e "${YELLOW}Creating postcss.config.js...${NC}"
cat > postcss.config.js << EOF
module.exports = {
  plugins: {
    'tailwindcss': {},
    'autoprefixer': {},
  }
}
EOF

# Install specific versions of Ajv and Ajv-keywords first to fix module issue
echo -e "${YELLOW}Installing Ajv packages to fix 'ajv/dist/compile/codegen' error...${NC}"
npm install --save-dev ajv@8.12.0 ajv-keywords@5.1.0 --legacy-peer-deps

# Installing frontend dependencies with specific versions to avoid compatibility issues
echo -e "${YELLOW}Installing frontend dependencies...${NC}"
npm install --legacy-peer-deps \
    react@18.2.0 \
    react-dom@18.2.0 \
    react-scripts@5.0.1 \
    @headlessui/react@1.7.17 \
    @heroicons/react@2.0.18 \
    axios@1.6.0 \
    chart.js@4.4.0 \
    react-chartjs-2@5.2.0 \
    react-syntax-highlighter@15.5.0 \
    @babel/core@7.23.3 \
    @babel/preset-env@7.23.3 \
    @babel/preset-react@7.23.3 

# Install dev dependencies separately
echo -e "${YELLOW}Installing frontend dev dependencies...${NC}"
npm install --legacy-peer-deps --save-dev \
    tailwindcss@3.3.5 \
    postcss@8.4.31 \
    autoprefixer@10.4.16

# Clean npm cache and run npm rebuild to fix potential binary issues
echo -e "${YELLOW}Cleaning npm cache and rebuilding dependencies...${NC}"
npm cache clean --force
npm rebuild

# Initialize tailwind with a specific configuration
echo -e "${YELLOW}Initializing Tailwind CSS...${NC}"
cat > tailwind.config.js << EOF
/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
    "./public/index.html"
  ],
  theme: {
    extend: {},
  },
  plugins: [],
}
EOF

# Create or ensure index.css exists with proper tailwind directives
if [ ! -f "src/styles/index.css" ]; then
    echo -e "${YELLOW}Creating index.css...${NC}"
    mkdir -p src/styles
    cat > src/styles/index.css << EOF
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

echo -e "${GREEN}✓ Frontend setup complete!${NC}"

# Return to project root
cd ..

# Create start script for each platform
echo -e "\n${YELLOW}Creating startup scripts...${NC}"

# Create cross-platform start script
cat > start.sh << 'EOF'
#!/bin/bash
# Cross-platform startup script for Data Analysis GUI

# Terminal colors for better readability
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Starting Data Analysis GUI               ${NC}"
echo -e "${BLUE}=============================================${NC}"

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

echo -e "${YELLOW}Detected operating system: ${OS}${NC}"

# Function to check if backend is ready
check_backend() {
    local max_attempts=30
    local attempt=1
    
    echo -e "${YELLOW}Waiting for backend to initialize...${NC}"
    while [ $attempt -le $max_attempts ]; do
        if curl -s http://localhost:8000 > /dev/null; then
            echo -e "${GREEN}✓ Backend is ready!${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
        attempt=$((attempt+1))
    done
    
    echo -e "\n${RED}Backend failed to start within the expected time.${NC}"
    return 1
}

# Check if virtual environment exists
VENV_EXISTS=false
if [[ "$OS" == "Windows" ]]; then
    if [ -d "backend/venv" ] && [ -f "backend/venv/Scripts/activate" ]; then
        VENV_EXISTS=true
    fi
else
    if [ -d "backend/venv" ] && [ -f "backend/venv/bin/activate" ]; then
        VENV_EXISTS=true
    fi
fi

if [ "$VENV_EXISTS" = false ]; then
    echo -e "${RED}Error: Virtual environment not found.${NC}"
    echo -e "${YELLOW}Please run the installation script first: ./install.sh${NC}"
    exit 1
fi

# Start backend server
echo -e "${YELLOW}Starting backend server...${NC}"
cd backend || { echo -e "${RED}Error: Failed to navigate to backend directory${NC}"; exit 1; }

# Check if app directory exists
if [ ! -d "app" ]; then
    echo -e "${RED}Error: 'app' directory not found in backend.${NC}"
    echo -e "${YELLOW}Please ensure the project structure is correct.${NC}"
    exit 1
fi

# Check if main.py exists
if [ ! -f "app/main.py" ]; then
    echo -e "${RED}Error: 'main.py' not found in backend/app directory.${NC}"
    echo -e "${YELLOW}Please ensure the project structure is correct.${NC}"
    exit 1
fi

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

# Check if backend started successfully
if ! check_backend; then
    echo -e "${RED}Failed to start backend server. Please check the error messages above.${NC}"
    if [[ "$OS" != "Windows" || "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
        # Kill the backend process if it's running but not responding
        kill $BACKEND_PID 2>/dev/null
    fi
    exit 1
fi

# Check if node_modules exists in frontend
if [ ! -d "frontend/node_modules" ]; then
    echo -e "${RED}Error: Node modules not found in frontend directory.${NC}"
    echo -e "${YELLOW}Please run the installation script first: ./install.sh${NC}"
    exit 1
fi

# Start frontend server
echo -e "${YELLOW}Starting frontend server...${NC}"
cd frontend || { echo -e "${RED}Error: Failed to navigate to frontend directory${NC}"; exit 1; }

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
echo -e "${YELLOW}Opening browser in 5 seconds...${NC}"
sleep 5

if [[ "$OS" == "Windows" && "$OSTYPE" != "msys" && "$OSTYPE" != "cygwin" ]]; then
    # For Windows (non-Bash shell)
    start http://localhost:3000
elif [[ "$OS" == "macOS" ]]; then
    # For macOS
    open http://localhost:3000
else
    # For Linux
    xdg-open http://localhost:3000 2>/dev/null || sensible-browser http://localhost:3000 2>/dev/null || open http://localhost:3000 2>/dev/null || echo -e "${YELLOW}Please open http://localhost:3000 in your browser${NC}"
fi

echo -e "${GREEN}✓ Data Analysis GUI is now running!${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop the servers${NC}"

# Wait for user to press Ctrl+C
trap "echo -e '${YELLOW}Shutting down servers...${NC}'; if [[ \"$OS\" != \"Windows\" || \"$OSTYPE\" == \"msys\" || \"$OSTYPE\" == \"cygwin\" ]]; then kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; fi; echo -e '${GREEN}Servers stopped${NC}'" INT

# Keep script running until Ctrl+C is pressed
if [[ "$OS" != "Windows" || "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    wait
fi
EOF

# Create Windows batch file for easier startup on Windows
cat > start.bat << 'EOF'
@echo off
echo Starting Data Analysis GUI...

REM Check if virtual environment exists
if not exist backend\venv\Scripts\activate (
    echo Error: Virtual environment not found.
    echo Please run the installation script first.
    exit /b 1
)

REM Check if app directory exists
if not exist backend\app (
    echo Error: 'app' directory not found in backend.
    echo Please ensure the project structure is correct.
    exit /b 1
)

REM Check if main.py exists
if not exist backend\app\main.py (
    echo Error: 'main.py' not found in backend\app directory.
    echo Please ensure the project structure is correct.
    exit /b 1
)

REM Check if node_modules exists in frontend
if not exist frontend\node_modules (
    echo Error: Node modules not found in frontend directory.
    echo Please run the installation script first.
    exit /b 1
)

REM Start backend server
echo Starting backend server...
start cmd /k "cd backend\app && ..\venv\Scripts\activate && uvicorn main:app --reload"

REM Wait for backend to start
echo Waiting for backend to initialize...
timeout /t 10 /nobreak

REM Check if backend is running
powershell -Command "try { $null = Invoke-WebRequest -Uri http://localhost:8000 -Method HEAD -UseBasicParsing; Write-Host 'Backend is running.' } catch { Write-Host 'Failed to connect to backend server. Please check for errors.'; exit 1 }"
if %ERRORLEVEL% NEQ 0 (
    echo Backend server failed to start. Please check the errors above.
    exit /b 1
)

REM Start frontend server
echo Starting frontend server...
start cmd /k "cd frontend && npm start"

REM Open browser after a short delay
echo Waiting for frontend to initialize...
timeout /t 10 /nobreak
start http://localhost:3000

echo Data Analysis GUI is now running!
echo Close the command windows to stop the servers when done.
EOF

# Make scripts executable
chmod +x start.sh

# Final instructions
echo -e "\n${GREEN}==================================================${NC}"
echo -e "${GREEN}✓ Installation complete!${NC}"
echo -e "${GREEN}==================================================${NC}"
echo -e "${YELLOW}To start the application:${NC}"
echo -e "  ${BLUE}• On macOS/Linux:${NC} ./start.sh"
echo -e "  ${BLUE}• On Windows:${NC} start.bat ${NC}or ${BLUE}./start.sh${NC} (if using Git Bash)"
echo -e "\n${YELLOW}The application will:${NC}"
echo -e "  1. Start the backend server on http://localhost:8000"
echo -e "  2. Start the frontend on http://localhost:3000"
echo -e "  3. Open your web browser automatically"
echo -e "${GREEN}==================================================${NC}"

# Ask if user wants to start the application now
read -p "Would you like to start the application now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${GREEN}Starting application...${NC}"
    if [[ "$OS" == "Windows" && "$OSTYPE" != "msys" && "$OSTYPE" != "cygwin" ]]; then
        start start.bat
    else
        ./start.sh
    fi
fi