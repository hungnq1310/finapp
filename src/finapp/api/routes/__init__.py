"""
API Routes Module
"""

from .crawler import router as crawler_router
from .extractor import router as extractor_router

__all__ = ["crawler_router", "extractor_router"]