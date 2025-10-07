"""
FastAPI Application Factory for Financial News Analysis

This module creates and configures the FastAPI application.
"""

import logging
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from finapp.api.routes import router

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app() -> FastAPI:
    """Create and configure FastAPI application"""
    
    app = FastAPI(
        title="Financial News Analysis Backend",
        description="API for financial news analysis with Windmill integration",
        version="1.0.0"
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Include API routes
    app.include_router(router)
    
    # Global exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        """Global exception handler"""
        logger.error(f"Unhandled exception: {exc}")
        return JSONResponse(
            status_code=500,
            content={
                "error": "INTERNAL_SERVER_ERROR",
                "message": "An unexpected error occurred",
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    
    # Startup/shutdown events
    @app.on_event("startup")
    async def startup_event():
        """Initialize services on startup"""
        logger.info("🚀 Starting Financial News Analysis Backend")
        logger.info("✅ Backend startup completed")
    
    @app.on_event("shutdown") 
    async def shutdown_event():
        """Cleanup on shutdown"""
        logger.info("🛑 Shutting down Financial News Analysis Backend")
        logger.info("✅ Backend shutdown completed")
    
    return app

# Create app instance
app = create_app()
