"""
API Router for Vietstock Crawler Service
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel
from typing import Optional, Dict, Any
import logging
import os
from datetime import datetime

from ...services.crawl import VietstockCrawlerService, CrawlerScheduler
from ...config import Config

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/crawl", tags=["Crawler"])

# Global instances (in production, these would be managed with proper DI)
_crawler_service: Optional[VietstockCrawlerService] = None
_scheduler: Optional[CrawlerScheduler] = None


def get_crawler_service():
    """Get or create crawler service instance"""
    global _crawler_service
    if _crawler_service is None:
        _crawler_service = VietstockCrawlerService()
    return _crawler_service


def get_scheduler() -> CrawlerScheduler:
    """Get or create scheduler instance"""
    global _scheduler
    if _scheduler is None:
        crawler = get_crawler_service()
        _scheduler = CrawlerScheduler(crawler)
    return _scheduler


# Response models
class CrawlerResponse(BaseModel):
    """Response model for crawler operations"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


class StatsResponse(BaseModel):
    """Response model for statistics"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


class SchedulerStatusResponse(BaseModel):
    """Response model for scheduler status"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


# Crawler control endpoints
@router.post("/start", response_model=CrawlerResponse)
async def start_crawler(
    background_tasks: BackgroundTasks,
    auto_schedule: bool = Query(True, description="Start with auto-scheduling"),
    interval_minutes: int = Query(5, ge=1, le=1440, description="Crawl interval in minutes"),
    extract_html: bool = Query(Config.CRAWLER_EXTRACT_HTML, description="Extract HTML content for articles"),
    filter_by_today: bool = Query(True, description="Only crawl articles from today")
):
    """
    Start the crawler service
    
    Args:
        background_tasks: FastAPI background tasks
        auto_schedule: Whether to start with auto-scheduling
        interval_minutes: Crawl interval in minutes
        extract_html: Whether to extract HTML content for articles
        filter_by_today: Whether to only crawl articles from today
        
    Returns:
        Starting result
    """
    try:
        crawler = get_crawler_service()
        
        if auto_schedule:
            scheduler = get_scheduler()
            scheduler.interval_minutes = interval_minutes
            scheduler.extract_html = extract_html
            scheduler.filter_by_today = filter_by_today
            
            # Start scheduler in background
            background_tasks.add_task(scheduler.start, run_immediately=True)
            
            return CrawlerResponse(
                success=True,
                message=f"Crawler started with auto-scheduling every {interval_minutes} minutes{' with HTML extraction' if extract_html else ''}",
                data={
                    "auto_schedule": True,
                    "interval_minutes": interval_minutes,
                    "extract_html": extract_html,
                    "filter_by_today": filter_by_today,
                    "output_directory": crawler.storage.output_dir
                }
            )
        else:
            # Run single crawl in background
            if extract_html:
                background_tasks.add_task(crawler.crawl_with_html_extraction, filter_by_today, extract_html)
            else:
                background_tasks.add_task(crawler.crawl_all_categories, filter_by_today)
            
            return CrawlerResponse(
                success=True,
                message="Single crawl job started",
                data={
                    "auto_schedule": False,
                    "output_directory": crawler.storage.output_dir
                }
            )
            
    except Exception as e:
        logger.error(f"Failed to start crawler: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start crawler: {e}")


@router.post("/stop", response_model=CrawlerResponse)
async def stop_crawler():
    """
    Stop the crawler scheduler
    
    Returns:
        Stop result
    """
    try:
        scheduler = get_scheduler()
        
        if not scheduler.is_running:
            return CrawlerResponse(
                success=False,
                message="Crawler scheduler is not running"
            )
        
        scheduler.stop()
        
        return CrawlerResponse(
            success=True,
            message="Crawler scheduler stopped successfully"
        )
        
    except Exception as e:
        logger.error(f"Failed to stop crawler: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to stop crawler: {e}")


@router.post("/trigger", response_model=CrawlerResponse)
async def trigger_manual_crawl():
    """
    Trigger a manual crawl job
    
    Returns:
        Trigger result
    """
    try:
        scheduler = get_scheduler()
        
        if not scheduler.is_running:
            return CrawlerResponse(
                success=False,
                message="Crawler scheduler is not running. Start the crawler first."
            )
        
        success = scheduler.trigger_manual_crawl()
        
        if success:
            return CrawlerResponse(
                success=True,
                message="Manual crawl triggered successfully"
            )
        else:
            return CrawlerResponse(
                success=False,
                message="Failed to trigger manual crawl"
            )
            
    except Exception as e:
        logger.error(f"Failed to trigger manual crawl: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger manual crawl: {e}")


# Statistics endpoints
@router.get("/stats", response_model=StatsResponse)
async def get_crawler_stats():
    """
    Get crawler statistics
    
    Returns:
        Crawler statistics
    """
    try:
        crawler = get_crawler_service()
        stats = crawler.get_crawl_statistics()
        
        return StatsResponse(
            success=True,
            message="Statistics retrieved successfully",
            data=stats
        )
        
    except Exception as e:
        logger.error(f"Failed to get statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get statistics: {e}")


@router.get("/scheduler/status", response_model=SchedulerStatusResponse)
async def get_scheduler_status():
    """
    Get scheduler status
    
    Returns:
        Scheduler status
    """
    try:
        scheduler = get_scheduler()
        status = scheduler.get_status()
        
        return SchedulerStatusResponse(
            success=True,
            message="Scheduler status retrieved successfully",
            data=status
        )
        
    except Exception as e:
        logger.error(f"Failed to get scheduler status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get scheduler status: {e}")


@router.put("/scheduler/interval", response_model=CrawlerResponse)
async def update_crawl_interval(
    interval_minutes: int = Query(..., ge=1, le=1440, description="New crawl interval in minutes")
):
    """
    Update crawl interval
    
    Args:
        interval_minutes: New crawl interval in minutes
        
    Returns:
        Update result
    """
    try:
        scheduler = get_scheduler()
        scheduler.update_interval(interval_minutes)
        
        return CrawlerResponse(
            success=True,
            message=f"Crawl interval updated to {interval_minutes} minutes",
            data={
                "new_interval_minutes": interval_minutes,
                "next_run_time": scheduler.get_next_run_time()
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to update interval: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update interval: {e}")


# Configuration endpoints
@router.get("/config", response_model=CrawlerResponse)
async def get_crawler_config():
    """
    Get crawler configuration
    
    Returns:
        Current configuration
    """
    try:
        crawler = get_crawler_service()
        scheduler = get_scheduler()
        
        config = {
            "base_url": crawler.base_url,
            "base_domain": crawler.base_domain,
            "output_directory": crawler.storage.output_dir,
            "database_name": crawler.storage.database_name,
            "storage_backend": "mongodb",
            "current_interval_minutes": scheduler.interval_minutes,
            "scheduler_running": scheduler.is_running
        }
        
        return CrawlerResponse(
            success=True,
            message="Configuration retrieved successfully",
            data=config
        )
        
    except Exception as e:
        logger.error(f"Failed to get configuration: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get configuration: {e}")


@router.post("/extract-html", response_model=CrawlerResponse)
async def extract_html_content(
    background_tasks: BackgroundTasks,
    date_filter: Optional[str] = Query(None, description="Date filter in YYYYMMDD format (default: today)")
):
    """
    Extract HTML content for existing articles
    
    Args:
        background_tasks: FastAPI background tasks
        date_filter: Specific date to extract HTML for (YYYYMMDD format)
        
    Returns:
        HTML extraction result
    """
    try:
        crawler = get_crawler_service()
        
        # Run crawl with HTML extraction
        background_tasks.add_task(crawler.crawl_with_html_extraction, True, True)
        
        return CrawlerResponse(
            success=True,
            message=f"HTML extraction job started for date: {date_filter or 'today'}",
            data={
                "date_filter": date_filter,
                "output_directory": crawler.storage.output_dir
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to start HTML extraction: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start HTML extraction: {e}")


@router.post("/restore-from-mongodb", response_model=CrawlerResponse)
async def restore_json_from_mongodb(
    date_filter: Optional[str] = Query(None, description="Date filter in YYYYMMDD format (default: today)")
):
    """
    Restore JSON files from MongoDB when they are missing or corrupted
    
    Args:
        date_filter: Specific date to restore (YYYYMMDD format, default: today)
        
    Returns:
        Restoration result
    """
    try:
        crawler = get_crawler_service()
        
        # Perform restoration
        restored = crawler.storage.restore_from_mongodb(date_filter)
        
        if restored:
            return CrawlerResponse(
                success=True,
                message=f"JSON file successfully restored from MongoDB for date: {date_filter or 'today'}",
                data={
                    "date_filter": date_filter,
                    "restored": True,
                    "output_directory": crawler.storage.output_dir
                }
            )
        else:
            return CrawlerResponse(
                success=False,
                message=f"Failed to restore JSON file from MongoDB for date: {date_filter or 'today'}",
                data={
                    "date_filter": date_filter,
                    "restored": False,
                    "reason": "No articles found in MongoDB or restoration failed"
                }
            )
        
    except Exception as e:
        logger.error(f"Failed to restore from MongoDB: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to restore from MongoDB: {e}")


@router.get("/check-json-status", response_model=CrawlerResponse)
async def check_json_file_status(
    date_filter: Optional[str] = Query(None, description="Date filter in YYYYMMDD format (default: today)")
):
    """
    Check if JSON file exists and can be restored from MongoDB
    
    Args:
        date_filter: Specific date to check (YYYYMMDD format, default: today)
        
    Returns:
        File status and restoration availability
    """
    try:
        crawler = get_crawler_service()
        
        # Check if JSON file exists
        file_exists = crawler.storage.ensure_json_file_exists(date_filter)
        
        # Get MongoDB stats for that date
        if date_filter:
            target_date = datetime.strptime(date_filter, "%Y%m%d").date()
        else:
            target_date = datetime.now().date()
        
        start_date = datetime.combine(target_date, datetime.min.time())
        end_date = datetime.combine(target_date, datetime.max.time())
        
        mongo_articles = crawler.storage.repository.find_articles_by_date_range(start_date, end_date)
        mongo_count = len(mongo_articles)
        
        # Determine file path
        date_str = target_date.strftime("%Y%m%d")
        daily_dir = os.path.join(crawler.storage.output_dir, date_str)
        articles_file = os.path.join(daily_dir, f"articles_{date_str}.json")
        
        return CrawlerResponse(
            success=True,
            message="File status check completed",
            data={
                "date_filter": date_filter,
                "file_exists": os.path.exists(articles_file),
                "file_path": articles_file,
                "mongo_articles_count": mongo_count,
                "restoration_possible": mongo_count > 0,
                "auto_restored": file_exists and mongo_count > 0
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to check JSON status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to check JSON status: {e}")


__all__ = ["router"]