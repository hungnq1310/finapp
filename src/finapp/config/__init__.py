"""
Configuration Package for Vietstock Crawler

This package provides configuration management with environment variable support.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Application configuration with environment variable support"""
    
    # Crawler Configuration
    CRAWLER_BASE_URL = os.getenv("CRAWLER_BASE_URL", "https://vietstock.vn/rss")
    CRAWLER_BASE_DOMAIN = os.getenv("CRAWLER_BASE_DOMAIN", "https://vietstock.vn")
    CRAWLER_OUTPUT_DIR = os.getenv("CRAWLER_OUTPUT_DIR", "data/vietstock")
    CRAWLER_DB_PATH = os.getenv("CRAWLER_DB_PATH", "data/vietstock_crawler.db")
    CRAWLER_INTERVAL_MINUTES = int(os.getenv("CRAWLER_INTERVAL_MINUTES", "5"))
    
    # API Configuration
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8002"))  # Changed to avoid port 8001 conflict
    API_RELOAD = os.getenv("API_RELOAD", "true").lower() == "true"
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Rate Limiting
    CRAWLER_RATE_LIMIT_DELAY = float(os.getenv("CRAWLER_RATE_LIMIT_DELAY", "1.0"))
    CRAWLER_SUBCATEGORY_DELAY = float(os.getenv("CRAWLER_RATE_LIMIT_DELAY", "0.5"))
    
    # Request timeout
    CRAWLER_REQUEST_TIMEOUT = int(os.getenv("CRAWLER_REQUEST_TIMEOUT", "30"))

__all__ = [
    "Config",
]
