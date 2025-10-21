"""
API Routes Module
"""

from .crawler import router as crawler_router
from .extractor import router as extractor_router
from .multi_crawler import router as multi_crawler_router
from .master_json import router as master_json_router
from .system_config import router as system_config_router
from .llm_results import router as llm_results_router
# from .financial_reports import router as financial_reports_router  # Temporarily disabled

__all__ = ["crawler_router", "extractor_router", "multi_crawler_router", "master_json_router", "system_config_router", "llm_results_router"]  # financial_reports_router