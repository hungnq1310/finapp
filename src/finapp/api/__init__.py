"""
API Package for Financial News Analysis

This package provides FastAPI-based REST API endpoints.
"""

from .app import app
from .models import (
    WindmillFlowRequest, WindmillFlowResponse,
    LLMStreamRequest, DatabaseQueryRequest, DatabaseInsertRequest,
    HealthCheckResponse, DatabaseResponse
)
from .routes import router

__all__ = [
    "app",
    "router",
    # Request models
    "WindmillFlowRequest",
    "LLMStreamRequest", 
    "DatabaseQueryRequest",
    "DatabaseInsertRequest",
    # Response models
    "WindmillFlowResponse",
    "HealthCheckResponse",
    "DatabaseResponse",
]
