# prefect_setup.py

import asyncio
import os
import logging
from pathlib import Path
from prefect import settings
from prefect.server.database import create_tables

logger = logging.getLogger(__name__)

async def setup_prefect(database_path=None):
    """
    Setup Prefect with persistent SQLite database
    
    Args:
        database_path: Path to SQLite database file. If None, uses default location.
    """
    try:
        # Set persistent SQLite database location
        if database_path:
            # Ensure the directory exists
            db_dir = os.path.dirname(database_path)
            if not os.path.exists(db_dir):
                os.makedirs(db_dir)
                
            # Set database URL
            os.environ["PREFECT_API_DATABASE_CONNECTION_URL"] = f"sqlite+aiosqlite:///{database_path}"
            logger.info(f"Using SQLite database at: {database_path}")
        else:
            # Use default location in user data directory
            data_dir = os.path.join(os.path.expanduser("~"), ".prefect", "workflow_data")
            if not os.path.exists(data_dir):
                os.makedirs(data_dir)
            
            db_path = os.path.join(data_dir, "prefect.db")
            os.environ["PREFECT_API_DATABASE_CONNECTION_URL"] = f"sqlite+aiosqlite:///{db_path}"
            logger.info(f"Using SQLite database at: {db_path}")
        
        # Configure other Prefect settings for persistence
        os.environ["PREFECT_LOGGING_LEVEL"] = "INFO"
        os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
        
        # Create database tables if they don't exist
        await create_tables()
        
        logger.info("✅ Prefect successfully configured with persistent SQLite database")
        return True
    except Exception as e:
        logger.error(f"❌ Error during Prefect setup: {str(e)}")
        return False

if __name__ == "__main__":
    # Run the setup asynchronously
    asyncio.run(setup_prefect())
