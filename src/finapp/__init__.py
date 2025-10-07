"""
Financial News Analysis Application Package

This is the main application package for the financial news analysis system.
It provides:
- Schema definitions and data models
- Database abstractions and implementations
- API endpoints and routing
- Windmill service integrations
- Configuration management
- Utility functions

The package is organized into logical modules:
- finapp.schema: Data models and schema definitions
- finapp.database: Database abstractions and MongoDB implementation
- finapp.api: FastAPI application and REST endpoints
- finapp.services: Service implementations (Windmill integration)
- finapp.config: Configuration management
- finapp.utils: Utility functions and helpers
"""

from finapp.api import app
from finapp.config import Config

__version__ = "1.0.0"

__all__ = [
    "app",
    "Config",
]
