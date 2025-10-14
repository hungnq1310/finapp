"""
API Models for Financial News Analysis

This module defines Pydantic models for API requests and responses.
"""

from datetime import datetime
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field


# Request Models
class WindmillFlowRequest(BaseModel):
    """Request model for triggering Windmill workflows"""
    workspace: str = Field(..., description="Windmill workspace name")
    flow_path: str = Field(..., description="Flow path in workspace")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Flow input parameters")
    async_mode: bool = Field(default=True, description="Run flow asynchronously")


class LLMStreamRequest(BaseModel):
    """Request model for LLM streaming via Windmill"""
    session_id: str
    messages: List[Dict[str, Any]]
    model: str = "gpt-4-turbo"
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    max_tokens: int = Field(default=512, ge=1, le=4096)
    stream: bool = True


class DatabaseQueryRequest(BaseModel):
    """Request model for database queries"""
    collection: str = Field(..., description="MongoDB collection name")
    query: Dict[str, Any] = Field(default_factory=dict, description="MongoDB query")
    projection: Optional[Dict[str, Any]] = Field(default=None, description="Fields to include/exclude")
    limit: int = Field(default=100, ge=1, le=1000)
    sort: Optional[Dict[str, int]] = Field(default=None, description="Sort order")


class DatabaseInsertRequest(BaseModel):
    """Request model for database inserts"""
    collection: str = Field(..., description="MongoDB collection name")
    document: Dict[str, Any] = Field(..., description="Document to insert")
    upsert: bool = Field(default=False, description="Upsert if document exists")


# Response Models
class WindmillFlowResponse(BaseModel):
    """Response model for workflow trigger"""
    success: bool
    workflow_id: Optional[str] = None
    correlation_id: Optional[str] = None
    error: Optional[str] = None
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class HealthCheckResponse(BaseModel):
    """Health check response model"""
    status: str
    timestamp: str
    services: Dict[str, Dict[str, Any]]
    uptime_seconds: float


class DatabaseResponse(BaseModel):
    """Response model for database operations"""
    success: bool
    message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class LLMExtractorResponese(BaseModel):
    """Response model for LLM Extractor"""
    # Fill out as needed
