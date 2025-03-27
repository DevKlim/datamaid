#!/bin/bash

# Script to create a macOS .app wrapper for the Data Analysis GUI Frontend
# This creates a clickable application in the Applications folder

# Terminal colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Creating macOS App for Data Analysis GUI   ${NC}"
echo -e "${BLUE}=============================================${NC}"

# Check if we're on macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    echo -e "${RED}Error: This script is for macOS only.${NC}"
    exit 1
fi

# Get the current directory (should be project root)
PROJECT_DIR="$(pwd)"

# Create application directory structure
echo -e "\n${YELLOW}Creating application structure...${NC}"
APP_NAME="Data Analysis GUI.app"
APP_PATH="/Applications/$APP_NAME"
CONTENTS_DIR="$APP_PATH/Contents"
MACOS_DIR="$CONTENTS_DIR/MacOS"
RESOURCES_DIR="$CONTENTS_DIR/Resources"

# Remove existing app if it exists
if [ -d "$APP_PATH" ]; then
    echo -e "${YELLOW}Removing existing application...${NC}"
    rm -rf "$APP_PATH"
fi

# Create the directory structure
mkdir -p "$MACOS_DIR"
mkdir -p "$RESOURCES_DIR"

# Create the Info.plist file
echo -e "${YELLOW}Creating Info.plist...${NC}"
cat > "$CONTENTS_DIR/Info.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleExecutable</key>
    <string>start_app</string>
    <key>CFBundleIdentifier</key>
    <string>com.dataanalysisgui.app</string>
    <key>CFBundleName</key>
    <string>Data Analysis GUI</string>
    <key>CFBundleIconFile</key>
    <string>AppIcon</string>
    <key>CFBundleShortVersionString</key>
    <string>1.0</string>
    <key>CFBundleInfoDictionaryVersion</key>
    <string>6.0</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleVersion</key>
    <string>1</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>NSAppTransportSecurity</key>
    <dict>
        <key>NSAllowsArbitraryLoads</key>
        <true/>
    </dict>
</dict>
</plist>
EOF

# Create the executable script
echo -e "${YELLOW}Creating executable script...${NC}"
cat > "$MACOS_DIR/start_app" << EOF
#!/bin/bash

# Get the directory where this script is located
DIR="\$( cd "\$( dirname "\${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="\$DIR/../../Resources/frontend"

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    osascript -e 'tell app "System Events" to display dialog "Node.js is not installed. Please install Node.js from https://nodejs.org/" buttons {"OK"} default button 1 with title "Data Analysis GUI" with icon caution'
    exit 1
fi

# Navigate to the frontend directory
cd "\$PROJECT_DIR"

# Install dependencies if node_modules doesn't exist
if [ ! -d "node_modules" ]; then
    echo "Installing dependencies..."
    npm install --legacy-peer-deps
fi

# Start the React development server
echo "Starting React application..."
npm start

exit 0
EOF

# Make the script executable
chmod +x "$MACOS_DIR/start_app"

# Copy the frontend directory to Resources
echo -e "${YELLOW}Copying frontend files...${NC}"
mkdir -p "$RESOURCES_DIR/frontend"
cp -R frontend/* "$RESOURCES_DIR/frontend/"

# Create simple icon (optional - replace with your own icon if available)
echo -e "${YELLOW}Creating application icon...${NC}"
# If you have a custom icon, copy it here:
# cp path/to/your/icon.icns "$RESOURCES_DIR/AppIcon.icns"

# Create a symlink to Applications folder for easy installation
echo -e "${YELLOW}Creating Applications folder symlink...${NC}"
ln -s /Applications "$PROJECT_DIR/Applications"

echo -e "\n${GREEN}==================================================${NC}"
echo -e "${GREEN}✓ macOS application creation complete!${NC}"
echo -e "${GREEN}==================================================${NC}"
echo -e "${YELLOW}The application has been installed to:${NC}"
echo -e "  ${BLUE}$APP_PATH${NC}"
echo -e "\n${YELLOW}You can launch it from:${NC}"
echo -e "  - Launchpad"
echo -e "  - Applications folder"
echo -e "  - Spotlight (Cmd+Space, then type 'Data Analysis GUI')"
echo -e "\n${YELLOW}NOTE:${NC} Make sure the backend server is running on port 8000"
echo -e "       before using the application."
echo -e "${GREEN}==================================================${NC}"

# Ask if user wants to launch the app now
read -p "Would you like to launch the application now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${GREEN}Launching application...${NC}"
    open "$APP_PATH"
fi
