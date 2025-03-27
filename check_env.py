import os
import sys
import platform
import subprocess

def check_command(command):
    """Check if a command is available in the system."""
    try:
        subprocess.run(
            command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            check=True, 
            shell=True
        )
        return True
    except subprocess.SubprocessError:
        return False

def check_directory(path):
    """Check if a directory exists and print its contents."""
    if os.path.exists(path):
        print(f"✓ Directory exists: {path}")
        print("  Contents:")
        for item in os.listdir(path):
            item_path = os.path.join(path, item)
            if os.path.isdir(item_path):
                print(f"    📁 {item}")
            else:
                print(f"    📄 {item}")
        return True
    else:
        print(f"✗ Directory missing: {path}")
        return False

def main():
    print("=" * 50)
    print("Environment Check for Data Analysis GUI")
    print("=" * 50)
    
    # Check Python version
    print(f"\nPython version: {sys.version}")
    
    # Check operating system
    system = platform.system()
    print(f"Operating system: {system}")
    
    # Check current directory
    current_dir = os.getcwd()
    print(f"Current directory: {current_dir}")
    
    # Check project structure
    print("\nChecking project structure:")
    backend_exists = check_directory("backend")
    if backend_exists:
        check_directory("backend/app")
        check_directory("backend/venv")
    
    frontend_exists = check_directory("frontend")
    if frontend_exists:
        check_directory("frontend/src")
    
    # Check key dependencies
    print("\nChecking key dependencies:")
    
    # Python packages
    print("\nPython packages:")
    for package in ["fastapi", "uvicorn", "pandas", "polars", "duckdb"]:
        try:
            module = __import__(package)
            print(f"✓ {package} is installed")
        except ImportError:
            print(f"✗ {package} is not installed")
    
    # Node.js and npm
    print("\nNode.js environment:")
    if check_command("node --version"):
        result = subprocess.run("node --version", shell=True, capture_output=True, text=True)
        print(f"✓ Node.js is installed: {result.stdout.strip()}")
    else:
        print("✗ Node.js is not installed or not in PATH")
    
    if check_command("npm --version"):
        result = subprocess.run("npm --version", shell=True, capture_output=True, text=True)
        print(f"✓ npm is installed: {result.stdout.strip()}")
    else:
        print("✗ npm is not installed or not in PATH")

    print("\n" + "=" * 50)
    print("Environment check complete")
    print("=" * 50)

if __name__ == "__main__":
    main()