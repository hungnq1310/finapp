"""
Services Package for Financial News Analysis

This package provides service implementations and abstractions for data processing.

Note: RSS collection services have been moved to finapp.crawl.services module.
"""

# Legacy services (keep existing ones)
try:
    from .windmill import WindmillService
    from .abstract import WorkflowOrchestrator
    
    __all__ = [
        "WindmillService", 
        "WorkflowOrchestrator",
    ]
except ImportError:
    # If legacy services don't exist
    __all__ = []
