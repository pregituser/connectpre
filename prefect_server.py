# prefect_server.py

import subprocess
import atexit
import time
import logging
import os
import signal
import sys

logger = logging.getLogger(__name__)

prefect_process = None

def start_prefect_server():
    """Start the Prefect API server in a subprocess"""
    global prefect_process
    
    if prefect_process:
        logger.info("Prefect server is already running")
        return
    
    try:
        # Start Prefect API server
        logger.info("Starting Prefect API server")
        prefect_process = subprocess.Popen(
            ["prefect", "server", "start"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        
        # Register cleanup function
        atexit.register(stop_prefect_server)
        
        # Wait for server to start
        time.sleep(2)
        
        # Check if the process is running
        if prefect_process.poll() is None:
            logger.info("Prefect API server started successfully")
            return True
        else:
            stdout, stderr = prefect_process.communicate()
            logger.error(f"Failed to start Prefect API server: {stderr}")
            prefect_process = None
            return False
    except Exception as e:
        logger.error(f"Error starting Prefect API server: {str(e)}")
        if prefect_process:
            prefect_process.terminate()
            prefect_process = None
        return False

def stop_prefect_server():
    """Stop the Prefect API server"""
    global prefect_process
    
    if prefect_process:
        logger.info("Stopping Prefect API server")
        try:
            # Try gentle termination first
            prefect_process.terminate()
            
            # Wait for process to terminate
            for _ in range(5):
                if prefect_process.poll() is not None:
                    break
                time.sleep(1)
            
            # If still running, kill it
            if prefect_process.poll() is None:
                prefect_process.kill()
                prefect_process.wait()
            
            logger.info("Prefect API server stopped")
        except Exception as e:
            logger.error(f"Error stopping Prefect API server: {str(e)}")
        finally:
            prefect_process = None

# Signal handler for graceful shutdown
def signal_handler(sig, frame):
    stop_prefect_server()
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
