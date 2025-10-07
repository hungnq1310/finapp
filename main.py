"""
Main Entry Point for Financial News Analysis API

This module starts the refactored FastAPI application.
"""

import uvicorn
from finapp.api import app

def main():
    """Main entry point"""
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        log_level="info",
        reload=True
    )

if __name__ == "__main__":
    main()
