# prefect_integration.py

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any

from prefect_setup import setup_prefect
from prefect_server import start_prefect_server, stop_prefect_server
from workflow_service_hooks import WorkflowServiceHooks
from fastapi_integration import integrate_prefect_with_routes

logger = logging.getLogger(__name__)

class PrefectIntegration:
    """
    Main Prefect integration class
    """
    
    def __init__(self, database_path=None):
        self.database_path = database_path
        self.initialized = False
    
    async def initialize(self):
        """Initialize Prefect integration"""
        if not self.initialized:
            # Start Prefect server
            server_started = start_prefect_server()
            if not server_started:
                logger.error("Failed to start Prefect server")
                raise RuntimeError("Failed to start Prefect server")
            
            # Setup Prefect with persistent SQLite
            setup_success = await setup_prefect(self.database_path)
            if not setup_success:
                logger.error("Failed to setup Prefect")
                stop_prefect_server()
                raise RuntimeError("Failed to setup Prefect")
            
            self.initialized = True
            logger.info("Prefect integration initialized successfully")
    
    def shutdown(self):
        """Shutdown Prefect integration"""
        stop_prefect_server()
        logger.info("Prefect integration shutdown")
    
    def get_workflow_hooks(self):
        """Get workflow service hooks"""
        return WorkflowServiceHooks
    
    def integrate_with_fastapi(self, app, workflowRunsRouter):
        """Integrate with FastAPI routes"""
        integrate_prefect_with_routes(app, workflowRunsRouter)
        logger.info("Prefect integrated with FastAPI routes")

# Create a lifespan manager for FastAPI
@asynccontextmanager
async def prefect_lifespan(app, database_path=None):
    """
    Lifespan manager for FastAPI to initialize Prefect
    
    Usage:
        app = FastAPI(lifespan=lambda app: prefect_lifespan(app))
    """
    # Initialize Prefect
    integration = PrefectIntegration(database_path)
    await integration.initialize()
    
    # Store the integration in the app state
    app.state.prefect_integration = integration
    
    yield
    
    # Shutdown Prefect
    integration.shutdown()
    logger.info("Prefect integration shutdown")

# Convenience function to get hooks for your workflow service
def get_workflow_hooks():
    """Get workflow service hooks"""
    return WorkflowServiceHooks
