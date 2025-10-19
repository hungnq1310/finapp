"""
API Router for Multi-Source Crawler Management

This module provides REST API endpoints for managing multiple news source crawlers
with unified interface and individual source control.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import logging

from ...services.crawl.factory import CrawlerFactory, MultiSourceCrawler
from ...config import Config

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/multi-crawl", tags=["Multi-Source Crawler"])

# Global instances
_multi_crawler: Optional[MultiSourceCrawler] = None


def get_multi_crawler() -> MultiSourceCrawler:
    """Get or create multi-source crawler instance"""
    global _multi_crawler
    if _multi_crawler is None:
        sources = Config.CRAWLER_SOURCES
        _multi_crawler = MultiSourceCrawler(sources=sources)
    return _multi_crawler


# Response models
class MultiCrawlerResponse(BaseModel):
    """Response model for multi-crawler operations"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


class SourcesListResponse(BaseModel):
    """Response model for available sources list"""
    available_sources: List[str]
    configured_sources: List[str]
    supported_sources: List[str]


class CrawlResultsResponse(BaseModel):
    """Response model for multi-source crawl results"""
    sources_crawled: List[str]
    total_articles: int
    source_results: Dict[str, Any]
    successful_sources: List[str]
    failed_sources: List[str]


# Management endpoints
@router.get("/sources", response_model=SourcesListResponse)
async def get_available_sources():
    """
    Get list of available and configured crawler sources
    
    Returns:
        Sources information including available, configured, and supported sources
    """
    try:
        available_sources = CrawlerFactory.get_available_sources()
        configured_sources = Config.CRAWLER_SOURCES
        supported_sources = list(Config.CRAWLER_SOURCE_CONFIGS.keys())
        
        return SourcesListResponse(
            available_sources=available_sources,
            configured_sources=configured_sources,
            supported_sources=supported_sources
        )
        
    except Exception as e:
        logger.error(f"❌ Failed to get sources list: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get sources: {e}")


@router.post("/crawl-all", response_model=MultiCrawlerResponse)
async def crawl_all_sources(
    background_tasks: BackgroundTasks,
    sources: Optional[List[str]] = Query(None, description="Specific sources to crawl (default: all configured)"),
    filter_by_today: bool = Query(True, description="Only crawl articles from today"),
    extract_html: bool = Query(False, description="Extract HTML content for articles")
):
    """
    Crawl articles from all configured sources
    
    Args:
        background_tasks: FastAPI background tasks
        sources: Specific sources to crawl (optional)
        filter_by_today: Whether to only get articles from today
        extract_html: Whether to extract HTML content for articles
        
    Returns:
        Multi-crawler operation result
    """
    try:
        # Create multi-crawler with specified sources
        target_sources = sources or Config.CRAWLER_SOURCES
        multi_crawler = MultiSourceCrawler(sources=target_sources)
        
        # Run crawl in background
        def crawl_background():
            try:
                results = multi_crawler.crawl_all_sources(filter_by_today, extract_html)
                
                total_articles = sum(result.get('articles_found', 0) for result in results.values())
                successful_sources = [source for source, result in results.items() if result.get('success', False)]
                failed_sources = [source for source, result in results.items() if not result.get('success', False)]
                
                logger.info(f"🎉 Multi-source crawl completed")
                logger.info(f"📊 Total articles: {total_articles}")
                logger.info(f"✅ Successful sources: {len(successful_sources)}")
                logger.info(f"❌ Failed sources: {len(failed_sources)}")
                
            except Exception as e:
                logger.error(f"❌ Background multi-crawl failed: {e}")
        
        background_tasks.add_task(crawl_background)
        
        return MultiCrawlerResponse(
            success=True,
            message=f"Multi-source crawl started for {len(target_sources)} sources",
            data={
                "sources": target_sources,
                "filter_by_today": filter_by_today,
                "extract_html": extract_html,
                "status": "started"
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Failed to start multi-source crawl: {e}")
        raise HTTPException(status_code=500, detail=f"Multi-crawl failed: {e}")


@router.get("/crawl-results", response_model=CrawlResultsResponse)
async def get_latest_crawl_results():
    """
    Get results from the latest multi-source crawl
    
    Returns:
        Latest crawl results with per-source breakdown
    """
    try:
        multi_crawler = get_multi_crawler()
        stats = multi_crawler.get_combined_statistics()
        
        # Extract source information
        source_stats = stats.get('source_stats', {})
        total_articles = stats.get('total_articles', 0)
        sources_crawled = list(source_stats.keys())
        
        # Classify successful/failed sources
        successful_sources = []
        failed_sources = []
        
        for source, stat in source_stats.items():
            if 'error' in stat:
                failed_sources.append(source)
            else:
                successful_sources.append(source)
        
        return CrawlResultsResponse(
            sources_crawled=sources_crawled,
            total_articles=total_articles,
            source_results=source_stats,
            successful_sources=successful_sources,
            failed_sources=failed_sources
        )
        
    except Exception as e:
        logger.error(f"❌ Failed to get crawl results: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get results: {e}")


@router.post("/crawl-source/{source_name}", response_model=MultiCrawlerResponse)
async def crawl_specific_source(
    source_name: str,
    background_tasks: BackgroundTasks,
    filter_by_today: bool = Query(True, description="Only crawl articles from today"),
    extract_html: bool = Query(False, description="Extract HTML content for articles")
):
    """
    Crawl articles from a specific source
    
    Args:
        source_name: Name of the source to crawl
        background_tasks: FastAPI background tasks
        filter_by_today: Whether to only get articles from today
        extract_html: Whether to extract HTML content for articles
        
    Returns:
        Single source crawl result
    """
    try:
        # Validate source
        available_sources = CrawlerFactory.get_available_sources()
        if source_name not in available_sources:
            raise HTTPException(status_code=404, detail=f"Source not found: {source_name}")
        
        # Create single-source crawler
        crawler = CrawlerFactory.create_crawler(source_name)
        
        # Run crawl in background
        def crawl_single_source():
            try:
                if extract_html:
                    result = crawler.crawl_with_html_extraction(filter_by_today, True)
                else:
                    result = crawler.crawl_all_categories(filter_by_today)
                
                articles_found = result.total_articles if hasattr(result, 'total_articles') else 0
                logger.info(f"✅ Single source crawl completed: {source_name}")
                logger.info(f"📊 Articles found: {articles_found}")
                
            except Exception as e:
                logger.error(f"❌ Single source crawl failed {source_name}: {e}")
        
        background_tasks.add_task(crawl_single_source)
        
        return MultiCrawlerResponse(
            success=True,
            message=f"Crawl started for source: {source_name}",
            data={
                "source": source_name,
                "filter_by_today": filter_by_today,
                "extract_html": extract_html,
                "status": "started"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to start single source crawl: {e}")
        raise HTTPException(status_code=500, detail=f"Single source crawl failed: {e}")


@router.get("/source/{source_name}/config")
async def get_source_config(source_name: str):
    """
    Get configuration for a specific source
    
    Args:
        source_name: Name of the source
        
    Returns:
        Source configuration details
    """
    try:
        # Validate source
        available_sources = CrawlerFactory.get_available_sources()
        if source_name not in available_sources:
            raise HTTPException(status_code=404, detail=f"Source not found: {source_name}")
        
        # Get source configuration
        crawler = CrawlerFactory.create_crawler(source_name)
        config = crawler.get_source_config()
        
        # Add environment-based config if available
        env_config = Config.CRAWLER_SOURCE_CONFIGS.get(source_name, {})
        
        return {
            "success": True,
            "message": f"Configuration retrieved for {source_name}",
            "data": {
                "source_name": source_name,
                "runtime_config": config,
                "environment_config": env_config,
                "supported_features": {
                    "categories": config.get("supports_categories", False),
                    "html_extraction": config.get("supports_html_extraction", True),
                    "date_filtering": config.get("date_filtering", True),
                    "vietnam_timezone": config.get("vietnam_timezone", True)
                }
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to get source config: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get config: {e}")


@router.post("/register-source")
async def register_new_source(
    source_name: str = Query(..., description="Name of the new source"),
    crawler_class_path: str = Query(..., description="Python module path for crawler class")
):
    """
    Register a new crawler source dynamically
    
    Args:
        source_name: Name for the new source
        crawler_class_path: Module path (e.g., "my_module.MyCrawler")
        
    Returns:
        Registration result
    """
    try:
        # TODO: Implement dynamic source registration
        # This would require importing the module and registering the class
        
        return MultiCrawlerResponse(
            success=False,
            message="Dynamic source registration not implemented yet",
            data={
                "source_name": source_name,
                "crawler_class_path": crawler_class_path,
                "note": "This feature requires implementation of dynamic module loading"
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Failed to register source: {e}")
        raise HTTPException(status_code=500, detail=f"Source registration failed: {e}")


__all__ = ["router"]