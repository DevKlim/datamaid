#!/bin/bash

# Data Analysis GUI Frontend Installer for macOS
# This script automates the setup of the frontend application

# Terminal colors for better readability
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Print banner
echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Data Analysis GUI - Frontend Installer     ${NC}"
echo -e "${BLUE}=============================================${NC}"

# Check if we're on macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    echo -e "${RED}Error: This installer is for macOS only.${NC}"
    exit 1
fi

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check Node.js installation
echo -e "\n${YELLOW}Checking prerequisites...${NC}"

if ! command_exists node; then
    echo -e "${RED}Node.js is not installed. Installing now...${NC}"
    
    # Check if Homebrew is installed
    if ! command_exists brew; then
        echo -e "${YELLOW}Installing Homebrew...${NC}"
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi
    
    # Install Node.js using Homebrew
    echo -e "${YELLOW}Installing Node.js...${NC}"
    brew install node
    
    if ! command_exists node; then
        echo -e "${RED}Failed to install Node.js. Please install it manually from https://nodejs.org/${NC}"
        exit 1
    fi
fi

# Get Node.js version
NODE_VERSION=$(node -v)
echo -e "${GREEN}✓ Node.js is installed (${NODE_VERSION})${NC}"

# Check npm installation
if ! command_exists npm; then
    echo -e "${RED}npm is not installed. Please install Node.js with npm.${NC}"
    exit 1
fi

# Get npm version
NPM_VERSION=$(npm -v)
echo -e "${GREEN}✓ npm is installed (${NPM_VERSION})${NC}"

# Navigate to frontend directory or create it if not exists
echo -e "\n${YELLOW}Setting up frontend directory...${NC}"
if [ ! -d "frontend" ]; then
    echo -e "${YELLOW}Creating frontend directory...${NC}"
    mkdir -p frontend
fi

cd frontend || { echo -e "${RED}Error: Could not navigate to frontend directory.${NC}"; exit 1; }

# Initialize package.json if it doesn't exist
if [ ! -f "package.json" ]; then
    echo -e "${YELLOW}Initializing npm project...${NC}"
    npm init -y
    
    # Update package.json with our project details
    cat > package.json << EOF
{
  "name": "data-analysis-gui-frontend",
  "version": "0.1.0",
  "private": true,
  "dependencies": {
    "@headlessui/react": "^1.7.17",
    "@heroicons/react": "^2.1.1",
    "axios": "^1.6.2",
    "chart.js": "^4.4.1",
    "react": "^18.2.0",
    "react-chartjs-2": "^5.2.0",
    "react-dom": "^18.2.0",
    "react-scripts": "5.0.1",
    "react-syntax-highlighter": "^15.5.0"
  },
  "scripts": {
    "start": "react-scripts start",
    "build": "react-scripts build",
    "test": "react-scripts test",
    "eject": "react-scripts eject"
  },
  "eslintConfig": {
    "extends": [
      "react-app",
      "react-app/jest"
    ]
  },
  "browserslist": {
    "production": [
      ">0.2%",
      "not dead",
      "not op_mini all"
    ],
    "development": [
      "last 1 chrome version",
      "last 1 firefox version",
      "last 1 safari version"
    ]
  },
  "devDependencies": {
    "autoprefixer": "^10.4.16",
    "postcss": "^8.4.31",
    "tailwindcss": "^3.3.5"
  },
  "proxy": "http://localhost:8000"
}
EOF
fi

# Create necessary directories if they don't exist
echo -e "${YELLOW}Creating directory structure...${NC}"
mkdir -p public src/components src/services src/styles

# Set up Tailwind CSS
echo -e "\n${YELLOW}Setting up Tailwind CSS...${NC}"
if [ ! -f "tailwind.config.js" ]; then
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
fi

if [ ! -f "postcss.config.js" ]; then
    cat > postcss.config.js << EOF
module.exports = {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
  },
}
EOF
fi

# Create index.css
if [ ! -f "src/styles/index.css" ]; then
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

# Create public/index.html if it doesn't exist
if [ ! -f "public/index.html" ]; then
    cat > public/index.html << EOF
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <link rel="icon" href="%PUBLIC_URL%/favicon.ico" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="theme-color" content="#000000" />
    <meta
      name="description"
      content="Data Analysis GUI - A graphical interface for pandas, polars, and SQL-like operations"
    />
    <link rel="apple-touch-icon" href="%PUBLIC_URL%/logo192.png" />
    <link rel="manifest" href="%PUBLIC_URL%/manifest.json" />
    <title>Data Analysis GUI</title>
  </head>
  <body>
    <noscript>You need to enable JavaScript to run this app.</noscript>
    <div id="root"></div>
  </body>
</html>
EOF
fi

# Create public/manifest.json if it doesn't exist
if [ ! -f "public/manifest.json" ]; then
    cat > public/manifest.json << EOF
{
  "short_name": "Data Analysis GUI",
  "name": "Data Analysis GUI for Pandas, Polars, and SQL",
  "icons": [
    {
      "src": "favicon.ico",
      "sizes": "64x64 32x32 24x24 16x16",
      "type": "image/x-icon"
    }
  ],
  "start_url": ".",
  "display": "standalone",
  "theme_color": "#000000",
  "background_color": "#ffffff"
}
EOF
fi

# Install dependencies
echo -e "\n${YELLOW}Installing npm dependencies...${NC}"
npm install --legacy-peer-deps

# Create launch script
echo -e "\n${YELLOW}Creating launch script...${NC}"
cd ..
cat > launch_frontend.command << EOF
#!/bin/bash
cd "\$(dirname "\$0")/frontend"
npm start
EOF

chmod +x launch_frontend.command

echo -e "\n${GREEN}==================================================${NC}"
echo -e "${GREEN}✓ Frontend installation complete!${NC}"
echo -e "${GREEN}==================================================${NC}"
echo -e "${YELLOW}To start the frontend:${NC}"
echo -e "  - Double-click the ${BLUE}launch_frontend.command${NC} file"
echo -e "  - Or run ${BLUE}cd frontend && npm start${NC} in your terminal"
echo -e "\n${YELLOW}NOTE:${NC} Make sure the backend server is running on port 8000"
echo -e "       before using the application."
echo -e "${GREEN}==================================================${NC}"

# Ask if user wants to start the frontend now
read -p "Would you like to start the frontend now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${GREEN}Starting frontend...${NC}"
    cd frontend
    npm start
fi
