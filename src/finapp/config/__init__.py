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
    CRAWLER_INTERVAL_MINUTES = int(os.getenv("CRAWLER_INTERVAL_MINUTES", "5"))
    
    # MongoDB Configuration
    MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    DATABASE_NAME = os.getenv("DATABASE_NAME", "financial_news")
    
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
    
    # HTML Content Extraction
    CRAWLER_EXTRACT_HTML = os.getenv("CRAWLER_EXTRACT_HTML", "false").lower() == "true"
    CRAWLER_HTML_EXTRACTION_DELAY = float(os.getenv("CRAWLER_HTML_EXTRACTION_DELAY", "2.0"))
    CRAWLER_HTML_BATCH_SIZE = int(os.getenv("CRAWLER_HTML_BATCH_SIZE", "10"))
    
    # LLM Extractor Configuration
    OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
    OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
    LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME", "anthropic/claude-3.5-sonnet")
    LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.1"))
    LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "4096"))
    EXTRACTOR_BATCH_SIZE = int(os.getenv("EXTRACTOR_BATCH_SIZE", "5"))
    EXTRACTOR_DELAY_SECONDS = float(os.getenv("EXTRACTOR_DELAY_SECONDS", "1.0"))
    EXTRACTOR_OUTPUT_DIR = os.getenv("EXTRACTOR_OUTPUT_DIR", "data/extracted")

__all__ = [
    "Config",
]
