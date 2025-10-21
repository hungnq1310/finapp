"""
Configuration Package for Vietstock Crawler

This package provides configuration management with environment variable support.
All configuration values should be set in .env file.
"""

import os
import logging
from dotenv import load_dotenv
from .env_validator import ConfigValidator, get_env

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


class Config:
    """
    Application configuration with environment variable support.
    
    All configuration values are loaded from environment variables.
    If a required variable is missing, a helpful error message is logged.
    """
    
    # MongoDB Configuration (REQUIRED)
    try:
        MONGODB_URI = ConfigValidator.get_required_env(
            "MONGODB_URI",
            "MongoDB connection URI",
            "mongodb://localhost:27017"
        )
    except Exception:
        logger.warning("MONGODB_URI not set, using default: mongodb://localhost:27017")
        MONGODB_URI = "mongodb://localhost:27017"
    
    try:
        DATABASE_NAME = ConfigValidator.get_required_env(
            "DATABASE_NAME",
            "MongoDB database name",
            "financial_news"
        )
    except Exception:
        logger.warning("DATABASE_NAME not set, using default: financial_news")
        DATABASE_NAME = "financial_news"
    
    # Multi-Source Crawler Configuration
    CRAWLER_SOURCES = get_env(
        "CRAWLER_SOURCES", 
        "vietstock",
        "Comma-separated list of crawler sources (vietstock,cafef,vietnamfinance)"
    ).split(",")
    
    CRAWLER_BASE_URL = get_env(
        "CRAWLER_BASE_URL",
        "https://vietstock.vn/rss",
        "Base URL for RSS feeds"
    )
    
    CRAWLER_BASE_DOMAIN = get_env(
        "CRAWLER_BASE_DOMAIN",
        "https://vietstock.vn",
        "Base domain for article URLs"
    )
    
    CRAWLER_OUTPUT_DIR = get_env(
        "CRAWLER_OUTPUT_DIR",
        "data",
        "Base directory for crawler output"
    )
    
    CRAWLER_INTERVAL_MINUTES = get_env(
        "CRAWLER_INTERVAL_MINUTES",
        5,
        "Interval between crawler runs in minutes",
        int
    )
    
    # Source-specific configurations
    CRAWLER_SOURCE_CONFIGS = {
        'vietstock': {
            'base_url': get_env("VIETSTOCK_BASE_URL", "https://vietstock.vn/rss", "Vietstock RSS base URL"),
            'base_domain': get_env("VIETSTOCK_BASE_DOMAIN", "https://vietstock.vn", "Vietstock base domain"),
            'output_dir': get_env("VIETSTOCK_OUTPUT_DIR", "data/vietstock", "Vietstock output directory"),
            'supports_categories': True,
            'html_extraction_delay': get_env("VIETSTOCK_HTML_DELAY", 2.0, "Vietstock HTML extraction delay", float),
            'html_batch_size': get_env("VIETSTOCK_HTML_BATCH_SIZE", 10, "Vietstock HTML batch size", int)
        },
        'cafef': {
            'base_url': get_env("CAFEF_BASE_URL", "https://cafef.vn/rss", "CafeF RSS base URL"),
            'base_domain': get_env("CAFEF_BASE_DOMAIN", "https://cafef.vn", "CafeF base domain"),
            'output_dir': get_env("CAFEF_OUTPUT_DIR", "data/cafef", "CafeF output directory"),
            'supports_categories': True,
            'html_extraction_delay': get_env("CAFEF_HTML_DELAY", 3.0, "CafeF HTML extraction delay", float),
            'html_batch_size': get_env("CAFEF_HTML_BATCH_SIZE", 5, "CafeF HTML batch size", int)
        },
        'vietnamfinance': {
            'base_url': get_env("VIETNAMFINANCE_BASE_URL", "https://vietnamfinance.vn/rss", "VietnamFinance RSS base URL"),
            'base_domain': get_env("VIETNAMFINANCE_BASE_DOMAIN", "https://vietnamfinance.vn", "VietnamFinance base domain"),
            'output_dir': get_env("VIETNAMFINANCE_OUTPUT_DIR", "data/vietnamfinance", "VietnamFinance output directory"),
            'supports_categories': True,
            'html_extraction_delay': get_env("VIETNAMFINANCE_HTML_DELAY", 2.5, "VietnamFinance HTML extraction delay", float),
            'html_batch_size': get_env("VIETNAMFINANCE_HTML_BATCH_SIZE", 8, "VietnamFinance HTML batch size", int)
        }
    }
    
    # API Configuration
    API_HOST = get_env("API_HOST", "0.0.0.0", "API host address")
    API_PORT = get_env("API_PORT", 8002, "API port number", int)
    API_RELOAD = get_env("API_RELOAD", True, "Enable API auto-reload", bool)
    
    # Logging
    LOG_LEVEL = get_env("LOG_LEVEL", "INFO", "Logging level")
    LOG_DIR = get_env("LOG_DIR", "data/extracted/logs", "Log file directory")
    
    # Rate Limiting
    CRAWLER_RATE_LIMIT_DELAY = get_env("CRAWLER_RATE_LIMIT_DELAY", 1.0, "Delay between requests in seconds", float)
    CRAWLER_SUBCATEGORY_DELAY = get_env("CRAWLER_SUBCATEGORY_DELAY", 0.5, "Delay between subcategory requests", float)
    
    # Request timeout
    CRAWLER_REQUEST_TIMEOUT = get_env("CRAWLER_REQUEST_TIMEOUT", 30, "Request timeout in seconds", int)
    
    # HTML Content Extraction
    CRAWLER_EXTRACT_HTML = get_env("CRAWLER_EXTRACT_HTML", False, "Enable HTML content extraction", bool)
    CRAWLER_HTML_EXTRACTION_DELAY = get_env("CRAWLER_HTML_EXTRACTION_DELAY", 2.0, "Delay for HTML extraction", float)
    CRAWLER_HTML_BATCH_SIZE = get_env("CRAWLER_HTML_BATCH_SIZE", 10, "HTML extraction batch size", int)
    
    # LLM Extractor Configuration
    OPENROUTER_API_KEY = get_env("OPENROUTER_API_KEY", "", "OpenRouter API key for LLM extraction")
    OPENROUTER_BASE_URL = get_env("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1", "OpenRouter API base URL")
    LLM_MODEL_NAME = get_env("LLM_MODEL_NAME", "anthropic/claude-3.5-sonnet", "LLM model name")
    LLM_TEMPERATURE = get_env("LLM_TEMPERATURE", 0.1, "LLM temperature", float)
    LLM_MAX_TOKENS = get_env("LLM_MAX_TOKENS", 4096, "LLM max tokens", int)
    
    # Extractor Configuration
    EXTRACTOR_BATCH_SIZE = get_env("EXTRACTOR_BATCH_SIZE", 5, "Extraction batch size", int)
    EXTRACTOR_DELAY_SECONDS = get_env("EXTRACTOR_DELAY_SECONDS", 1.0, "Delay between extractions", float)
    EXTRACTOR_OUTPUT_DIR = get_env("EXTRACTOR_OUTPUT_DIR", "data/extracted", "Extraction output directory")
    
    # Master JSON Configuration
    MASTER_JSON_DIR = get_env("MASTER_JSON_DIR", "data/master", "Master JSON output directory")
    
    @classmethod
    def validate_required_config(cls):
        """
        Validate that all required configuration is present.
        Logs helpful error messages if anything is missing.
        """
        required_for_llm = {
            "OPENROUTER_API_KEY": "API key for OpenRouter LLM service"
        }
        
        # Only validate LLM config if trying to use LLM features
        if cls.OPENROUTER_API_KEY:
            logger.info("✅ LLM configuration validated")
        else:
            logger.warning(
                "⚠️  OPENROUTER_API_KEY not set. LLM extraction features will not work.\n"
                "   Please add OPENROUTER_API_KEY to your .env file to enable LLM extraction."
            )
    
    @classmethod
    def log_configuration(cls):
        """Log current configuration (masking sensitive values)"""
        config_dict = {
            "MONGODB_URI": cls.MONGODB_URI,
            "DATABASE_NAME": cls.DATABASE_NAME,
            "CRAWLER_OUTPUT_DIR": cls.CRAWLER_OUTPUT_DIR,
            "EXTRACTOR_OUTPUT_DIR": cls.EXTRACTOR_OUTPUT_DIR,
            "API_HOST": cls.API_HOST,
            "API_PORT": cls.API_PORT,
            "LOG_LEVEL": cls.LOG_LEVEL,
            "LLM_MODEL_NAME": cls.LLM_MODEL_NAME,
            "OPENROUTER_API_KEY": cls.OPENROUTER_API_KEY,
        }
        
        ConfigValidator.log_configuration(
            config_dict,
            mask_keys=['key', 'password', 'secret', 'token', 'uri']
        )

__all__ = [
    "Config",
]
