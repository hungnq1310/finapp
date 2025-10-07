"""
Configuration Package for Financial News Analysis

This package provides configuration management.
"""

# Configuration would be implemented here
# For now, keeping it simple

class Config:
    """Application configuration"""
    MONGODB_URI = "mongodb://localhost:27017"
    DATABASE_NAME = "financial_news"
    WINDMILL_BASE_URL = "http://localhost:8000"
    WINDMILL_TOKEN = ""
    API_HOST = "0.0.0.0"
    API_PORT = 8001

__all__ = [
    "Config",
]
