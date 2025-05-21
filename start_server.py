import os
import sys
import signal
import psutil
import uvicorn

def kill_uvicorn_processes():
    """Kill any existing uvicorn processes"""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if 'uvicorn' in proc.info['name'] or \
               (proc.info['cmdline'] and 'uvicorn' in ' '.join(proc.info['cmdline'])):
                os.kill(proc.info['pid'], signal.SIGTERM)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

def main():
    # Kill existing uvicorn processes
    kill_uvicorn_processes()

    # Get the absolute path to the backend directory
    backend_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend')
    
    # Change to the backend directory
    os.chdir(backend_dir)
    
    # Add backend directory to Python path
    sys.path.insert(0, backend_dir)
    
    # Start the server
    port = 8000
    try:
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=port,
            reload=True,
            reload_dirs=[backend_dir]
        )
    except OSError as e:
        if "Address already in use" in str(e):
            print(f"Port {port} is in use, trying port 8001...")
            port = 8001
            uvicorn.run(
                "main:app",
                host="0.0.0.0",
                port=port,
                reload=True,
                reload_dirs=[backend_dir]
            )

if __name__ == "__main__":
    main() 