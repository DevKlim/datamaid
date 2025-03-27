#!/bin/bash

# Exit on any error
set -e

# Print commands before executing
set -x

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is not installed. Please install it first."
    exit 1
fi

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "Node.js is not installed. Please install it first."
    exit 1
fi

# Check if npm is installed
if ! command -v npm &> /dev/null; then
    echo "npm is not installed. Please install it first."
    exit 1
fi

# Print current directory
echo "Current directory: $(pwd)"

# Create project directories if they don't exist
mkdir -p backend/app
mkdir -p frontend/src/components
mkdir -p frontend/src/services
mkdir -p frontend/src/styles
mkdir -p frontend/public

echo "Setting up backend..."
cd backend

# Create Python virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install backend dependencies
pip install fastapi uvicorn python-multipart pandas polars duckdb pydantic numpy

# Save dependencies to requirements.txt
pip freeze > requirements.txt

echo "Backend setup complete!"

# Go back to project root
cd ..

echo "Setting up frontend..."
cd frontend

# Initialize npm project (non-interactive)
npm init -y

# Install frontend dependencies
npm install react react-dom react-scripts @headlessui/react @heroicons/react axios chart.js react-chartjs-2 react-syntax-highlighter tailwindcss@latest postcss autoprefixer

# Initialize tailwind
npx tailwindcss init -p

echo "Frontend setup complete!"

# Go back to project root
cd ..

echo "Installation complete! Now run the application with:"
echo "python run.py"