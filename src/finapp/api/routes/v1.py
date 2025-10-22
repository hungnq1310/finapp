"""
API Routes for Financial News Analysis

This module defines all FastAPI routes and endpoints.
Simplified version for the refactored structure.
"""

import uuid
import logging
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Depends, Query

from finapp.schema.request import (
    WindmillFlowRequest, WindmillFlowResponse,
    LLMStreamRequest, DatabaseQueryRequest, DatabaseInsertRequest, HealthCheckResponse
)
from finapp.schema.index import (
    IndexReport, IndexReportListItem, IndexReportListResponse,
)
from finapp.services.database.index_report import MinioService

async def get_minio_service() -> MinioService:
    """Dependency to get MinioService instance"""
    minio_service = MinioService()
    await minio_service.connect()
    return minio_service

logger = logging.getLogger(__name__)

# Create API router
router = APIRouter()

# Health check endpoints
@router.get("/", tags=["health"])
async def root():
    """Root endpoint"""
    return {
        "service": "Financial News Analysis Backend",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints": {
            "health": "/health",
            "windmill": "/windmill/*",
            "database": "/database/*",
            "docs": "/docs"
        }
    }

@router.get("/health", response_model=HealthCheckResponse, tags=["health"])
async def health_check():
    """Comprehensive health check"""
    # Implementation would check all services
    return HealthCheckResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat(),
        uptime_seconds=3600.0,
        services={
            "database": {"status": "healthy"},
            "windmill": {"status": "healthy"}
        }
    )

# Windmill workflow endpoints (simplified - would integrate with actual service)
@router.post("/windmill/trigger", response_model=WindmillFlowResponse, tags=["windmill"])
async def trigger_windmill_flow(request: WindmillFlowRequest):
    """Trigger a Windmill workflow"""
    # Mock implementation - would call actual WindmillService
    return WindmillFlowResponse(
        success=True,
        workflow_id=str(uuid.uuid4()),
        correlation_id=str(uuid.uuid4())
    )

@router.post("/windmill/trigger/news-crawling", tags=["windmill"])
async def trigger_news_crawling():
    """Trigger news crawling workflow"""
    return WindmillFlowResponse(
        success=True,
        workflow_id=str(uuid.uuid4()),
        correlation_id=str(uuid.uuid4())
    )

@router.post("/windmill/trigger/stock-analysis", tags=["windmill"])
async def trigger_stock_analysis(
    time_window: str = "current",
    companies: Optional[List[str]] = None
):
    """Trigger stock analysis workflow"""
    return WindmillFlowResponse(
        success=True,
        workflow_id=str(uuid.uuid4()),
        correlation_id=str(uuid.uuid4())
    )

@router.post("/windmill/trigger/sector-analysis", tags=["windmill"])
async def trigger_sector_analysis(sector: str = "technology"):
    """Trigger sector analysis workflow"""
    return WindmillFlowResponse(
        success=True,
        workflow_id=str(uuid.uuid4()),
        correlation_id=str(uuid.uuid4())
    )

@router.post("/windmill/trigger/market-overview", tags=["windmill"])
async def trigger_market_overview():
    """Trigger market overview workflow"""
    return WindmillFlowResponse(
        success=True,
        workflow_id=str(uuid.uuid4()),
        correlation_id=str(uuid.uuid4())
    )

@router.post("/windmill/llm-stream", tags=["windmill"])
async def windmill_llm_stream(request: LLMStreamRequest):
    """Stream LLM request through Windmill"""
    # Mock implementation - would call actual WindmillService
    return {"success": True, "result": "LLM response"}

# Database endpoints (simplified)
@router.post("/database/query", tags=["database"])
async def query_database(request: DatabaseQueryRequest):
    """Query database with filters"""
    return {
        "success": True,
        "documents": [],
        "count": 0
    }

@router.post("/database/insert", tags=["database"])
async def insert_database(request: DatabaseInsertRequest):
    """Insert document into database"""
    return {
        "success": True,
        "document_id": str(uuid.uuid4())
    }

# Convenience endpoints
@router.get("/news/articles", tags=["data"])
async def get_news_articles(
    limit: int = 10,
    company_id: Optional[str] = None,
    sentiment_min: Optional[float] = None
):
    """Get news articles with optional filters"""
    query = {}
    if company_id:
        query["companies_mentioned.company_id"] = company_id
    if sentiment_min is not None:
        query["sentiment.overall_sentiment"] = {"$gte": sentiment_min}
    
    # This would use the database service
    return {
        "success": True,
        "documents": [],
        "count": 0
    }

@router.get("/reports/stocks", tags=["data"])
async def get_stock_reports(limit: int = 10, ticker: Optional[str] = None):
    """Get stock analysis reports"""
    query = {}
    if ticker:
        query["ticker"] = ticker
    
    return {
        "success": True,
        "documents": [],
        "count": 0
    }

@router.get("/reports/sectors", tags=["data"])
async def get_sector_reports(limit: int = 10):
    """Get sector analysis reports"""
    return {
        "success": True,
        "documents": [],
        "count": 0
    }

@router.get("/reports/market", tags=["data"])
async def get_market_reports(limit: int = 10):
    """Get market overview reports"""
    return {
        "success": True,
        "documents": [],
        "count": 0
    }

# Index Report Endpoints (MinIO Integration)
@router.get("/reports/indices", tags=["index-reports"])
async def list_index_reports(
    limit: int = Query(default=10, description="Maximum number of reports to return"),
    minio_service: MinioService = Depends(get_minio_service)
):
    """List all available index reports from MinIO"""
    try:
        # Get list of reports from MinIO
        objects = minio_service.list_index_reports(limit=limit)
        print(objects)

        # Transform to response format
        reports = []
        for obj in objects:
            # Extract timestamp from filename if possible
            timestamp = ""
            filename = obj["object_name"]
            
            # Try to parse timestamp from filename pattern: stock_report_YYYYMMDD_HHMMSS.json
            if "stock_report_" in filename:
                try:
                    parts = filename.replace("stock_report_", "").replace(".json", "").split("_")
                    if len(parts) >= 2:
                        date_part = parts[0]  # YYYYMMDD
                        time_part = parts[1] if len(parts) > 1 else "000000"  # HHMMSS
                        timestamp_str = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}T{time_part[:2]}:{time_part[2:4]}:{time_part[4:6]}"
                        timestamp = timestamp_str
                except Exception:
                    timestamp = obj.get("last_modified", "")

            report_item = IndexReportListItem(
                filename=filename,
                timestamp=timestamp,
                size_bytes=obj["size"],
                last_modified=obj.get("last_modified")
            )
            reports.append(report_item)

        return IndexReportListResponse(
            reports=reports,
            total_count=len(reports),
            has_more=len(reports) == limit
        )
        
    except Exception as e:
        logger.error(f"Error listing index reports: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve index reports")


@router.get("/reports/indices/latest", tags=["index-reports"])
async def get_latest_index_report(
    minio_service: MinioService = Depends(get_minio_service)
):
    """Get the most recent index report"""
    try:
        report_data = minio_service.get_latest_index_report()
        
        if not report_data:
            raise HTTPException(status_code=404, detail="No index reports found")

        return report_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting latest index report: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve latest index report")


@router.get("/reports/indices/{filename}", tags=["index-reports"])
async def get_index_report_by_filename(
    filename: str,
    minio_service: MinioService = Depends(get_minio_service)
):
    """Get a specific index report by filename"""
    try:
        report_data = minio_service.get_index_report(filename)

        if not report_data:
            raise HTTPException(status_code=404, detail=f"Index report '{filename}' not found")

        return report_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting stock report {filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve stock report '{filename}'")


@router.get("/reports/indices/date/{date}", tags=["index-reports"])
async def get_index_report_by_date(
    date: str,
    minio_service: MinioService = Depends(get_minio_service)
):
    """Get index report for a specific date (format: YYYYMMDD)"""
    try:
        # Validate date format
        if len(date) != 8 or not date.isdigit():
            raise HTTPException(status_code=400, detail="Date must be in YYYYMMDD format")
        
        report_data = minio_service.get_index_report_by_date(date)
        
        if not report_data:
            raise HTTPException(status_code=404, detail=f"No index report found for date {date}")

        return report_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting index report for date {date}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve index report for date {date}")
