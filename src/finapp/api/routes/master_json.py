"""
API Router for Master JSON Data Management

This module provides REST API endpoints for managing and querying the daily master JSON files
that aggregate all extraction results with 3-level organization and full metadata.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

from ...services.extract.master_json_service import MasterJSONService
from ...services.extract.extraction_service import ExtractionService
from ...services.extract.extrator_agent import LLMExtractorAgent
from ...schema.request import LLMExtractorResponse
from ...config import Config

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/master", tags=["Master JSON"])

# Global instances
_master_service: Optional[MasterJSONService] = None
_extraction_service: Optional[ExtractionService] = None
_extractor_agent: Optional[LLMExtractorAgent] = None


def get_master_service() -> MasterJSONService:
    """Get or create master service instance"""
    global _master_service
    if _master_service is None:
        _master_service = MasterJSONService()
    return _master_service


def get_extraction_service() -> ExtractionService:
    """Get or create extraction service instance"""
    global _extraction_service
    if _extraction_service is None:
        _extraction_service = ExtractionService()
    return _extraction_service


def get_extractor_agent() -> LLMExtractorAgent:
    """Get or create extractor agent instance"""
    global _extractor_agent
    if _extractor_agent is None:
        _extractor_agent = LLMExtractorAgent()
    return _extractor_agent


# Request Models
class MasterQueryRequest(BaseModel):
    """Request model for querying master data"""
    tickers: Optional[List[str]] = Field(None, description="Filter by stock tickers")
    sectors: Optional[List[str]] = Field(None, description="Filter by sectors")
    sentiments: Optional[List[str]] = Field(None, description="Filter by sentiments")
    market_moving_only: bool = Field(False, description="Filter for market moving articles only")
    min_confidence: Optional[float] = Field(None, ge=0, le=1, description="Minimum confidence score")
    include_full_content: bool = Field(False, description="Include full article content")
    limit: Optional[int] = Field(None, ge=1, le=1000, description="Limit number of results")


class BatchProcessRequest(BaseModel):
    """Request model for batch processing daily articles"""
    target_date: str = Field(..., description="Target date in YYYY-MM-DD format")
    session_name: Optional[str] = Field(None, description="Optional session name")
    auto_organize: bool = Field(True, description="Auto-organize into master JSON")
    delay_seconds: Optional[float] = Field(None, ge=0, description="Delay between extractions")


# Response Models
class MasterSummaryResponse(BaseModel):
    """Response model for master data summary"""
    date: str
    metadata: Dict[str, Any]
    summary: Dict[str, Any]
    total_articles: int


class QueryResultsResponse(BaseModel):
    """Response model for query results"""
    success: bool
    date: str
    metadata: Dict[str, Any]
    summary: Dict[str, Any]
    query_summary: Dict[str, Any]
    articles: List[Dict[str, Any]]


class StockAnalysisResponse(BaseModel):
    """Response model for stock analysis"""
    success: bool
    date: str
    ticker: str
    analysis: Dict[str, Any]
    articles: List[Dict[str, Any]]


class SectorAnalysisResponse(BaseModel):
    """Response model for sector analysis"""
    success: bool
    date: str
    sector: str
    analysis: Dict[str, Any]
    articles: List[Dict[str, Any]]


# Main endpoints
@router.post("/process-daily", response_model=LLMExtractorResponse)
async def process_daily_to_master(
    background_tasks: BackgroundTasks,
    request: BatchProcessRequest
):
    """
    Process all articles for a date and organize into master JSON
    
    Args:
        request: Batch processing request
        
    Returns:
        Processing results
    """
    try:
        # Validate date format
        try:
            datetime.strptime(request.target_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
        
        logger.info(f"🚀 Starting daily processing for {request.target_date}")
        
        if request.auto_organize:
            # Run in background with master JSON organization
            def process_and_organize():
                try:
                    from pathlib import Path
                    import json
                    from ...config import Config
                    
                    # Find the JSON file for the date
                    config = Config()
                    vietstock_config = config.CRAWLER_SOURCE_CONFIGS.get('vietstock', {})
                    vietstock_output_dir = vietstock_config.get('output_dir', 'data/vietstock')
                    json_file = Path(f"{vietstock_output_dir}/{request.target_date.replace('-', '')}/articles_{request.target_date.replace('-', '')}.json")
                    
                    if not json_file.exists():
                        logger.error(f"No articles file found for {request.target_date}")
                        return
                    
                    # Load articles
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    articles = data.get("articles", [])
                    if not articles:
                        logger.warning(f"No articles found for {request.target_date}")
                        return
                    
                    # Extract and organize
                    master_service = get_master_service()
                    extractor = get_extractor_agent()
                    
                    successful_extractions = 0
                    
                    for article in articles:
                        try:
                            # Only process articles with HTML content
                            if not article.get("html_extraction_success", False):
                                continue
                            
                            # Extract data
                            extraction_result = extractor.extract_single_article(
                                title=article.get("title", ""),
                                category=article.get("category", ""),
                                description_text=article.get("description_text", ""),
                                main_content=article.get("main_content", ""),
                                article_guid=article.get("guid", "")
                            )
                            
                            # Append to master JSON
                            master_result = master_service.append_extraction_to_master(
                                target_date=request.target_date,
                                extraction_result=extraction_result.dict(),
                                article_metadata=article
                            )
                            
                            if master_result["success"]:
                                successful_extractions += 1
                                logger.info(f"Processed article {successful_extractions}/{len(articles)}: {article.get('title', '')[:50]}...")
                            
                            # Rate limiting
                            if request.delay_seconds and request.delay_seconds > 0:
                                import time
                                time.sleep(request.delay_seconds)
                                
                        except Exception as e:
                            logger.error(f"Failed to process article {article.get('guid', 'unknown')}: {e}")
                            continue
                    
                    logger.info(f"Daily processing completed for {request.target_date}: {successful_extractions}/{len(articles)} successful")
                    
                except Exception as e:
                    logger.error(f"Background processing failed for {request.target_date}: {e}")
            
            background_tasks.add_task(process_and_organize)
            
            return LLMExtractorResponse(
                success=True,
                message=f"Daily processing started for {request.target_date}. Results will be organized into master JSON.",
                data={
                    "target_date": request.target_date,
                    "session_name": request.session_name,
                    "auto_organize": True,
                    "status": "processing_started"
                }
            )
        else:
            # Run immediately without organization
            from ...config import Config
            config = Config()
            vietstock_config = config.CRAWLER_SOURCE_CONFIGS.get('vietstock', {})
            vietstock_output_dir = vietstock_config.get('output_dir', 'data/vietstock')
            
            service = get_extraction_service()
            result = service.process_articles_from_json(
                json_file_path=f"{vietstock_output_dir}/{request.target_date.replace('-', '')}/articles_{request.target_date.replace('-', '')}.json",
                session_name=request.session_name or f"daily-{request.target_date}"
            )
            return result
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Daily processing failed: {e}")
        raise HTTPException(status_code=500, detail=f"Daily processing failed: {e}")


@router.get("/summary/{target_date}", response_model=MasterSummaryResponse)
async def get_daily_summary(target_date: str):
    """
    Get daily summary from master JSON
    
    Args:
        target_date: Date in YYYY-MM-DD format
        
    Returns:
        Daily summary with metadata
    """
    try:
        # Validate date format
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
        
        master_service = get_master_service()
        result = master_service.query_master_data(target_date)
        
        if not result["success"]:
            if "error" in result and "No master data found" in result["error"]:
                # Try to process the date if master data doesn't exist
                return {
                    "success": False,
                    "message": f"No master data found for {target_date}. Use /master/process-daily to create it.",
                    "suggestion": f"POST /master/process-daily with target_date={target_date}"
                }
            raise HTTPException(status_code=404, detail=result["error"])
        
        return MasterSummaryResponse(
            date=target_date,
            metadata=result["metadata"],
            summary=result["summary"],
            total_articles=result["total_articles"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get daily summary for {target_date}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get daily summary")


@router.post("/query/{target_date}", response_model=QueryResultsResponse)
async def query_master_data(target_date: str, request: MasterQueryRequest):
    """
    Query master data with filters
    
    Args:
        target_date: Date in YYYY-MM-DD format
        request: Query parameters
        
    Returns:
        Filtered articles with metadata
    """
    try:
        # Validate date format
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
        
        master_service = get_master_service()
        
        # Convert request to query params dict
        query_params = {
            "tickers": request.tickers,
            "sectors": request.sectors,
            "sentiments": request.sentiments,
            "market_moving_only": request.market_moving_only,
            "min_confidence": request.min_confidence,
            "include_full_content": request.include_full_content,
            "limit": request.limit
        }
        
        # Remove None values
        query_params = {k: v for k, v in query_params.items() if v is not None}
        
        result = master_service.query_master_data(target_date, query_params)
        
        if not result["success"]:
            raise HTTPException(status_code=404, detail=result["error"])
        
        return QueryResultsResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to query master data for {target_date}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to query master data")


@router.get("/stock/{target_date}/{ticker}", response_model=StockAnalysisResponse)
async def get_stock_analysis(
    target_date: str,
    ticker: str,
    include_full_content: bool = Query(False, description="Include full article content")
):
    """
    Get comprehensive analysis for a specific stock
    
    Args:
        target_date: Date in YYYY-MM-DD format
        ticker: Stock ticker symbol
        include_full_content: Whether to include full article content
        
    Returns:
        Stock analysis with articles and insights
    """
    try:
        # Validate date format
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
        
        master_service = get_master_service()
        result = master_service.get_stock_analysis(target_date, ticker.upper(), include_full_content)
        
        if not result["success"]:
            raise HTTPException(status_code=404, detail=result["error"])
        
        return StockAnalysisResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get stock analysis for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get stock analysis")


@router.get("/sector/{target_date}/{sector_name}", response_model=SectorAnalysisResponse)
async def get_sector_analysis(
    target_date: str,
    sector_name: str,
    include_full_content: bool = Query(False, description="Include full article content")
):
    """
    Get comprehensive analysis for a specific sector
    
    Args:
        target_date: Date in YYYY-MM-DD format
        sector_name: Sector name (URL encoded)
        include_full_content: Whether to include full article content
        
    Returns:
        Sector analysis with articles and insights
    """
    try:
        # Validate date format
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
        
        # Decode sector name (URL encoded)
        from urllib.parse import unquote
        sector_decoded = unquote(sector_name)
        
        master_service = get_master_service()
        result = master_service.get_sector_analysis(target_date, sector_decoded, include_full_content)
        
        if not result["success"]:
            raise HTTPException(status_code=404, detail=result["error"])
        
        return SectorAnalysisResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get sector analysis for {sector_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get sector analysis")


@router.get("/available-dates")
async def get_available_dates():
    """
    Get list of all available dates with master data
    
    Returns:
        List of available dates with basic stats
    """
    try:
        master_service = get_master_service()
        result = master_service.get_available_dates()
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to get available dates: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get available dates")


@router.post("/export-report/{target_date}")
async def export_report_data(
    target_date: str,
    tickers: Optional[List[str]] = Query(None, description="Tickers to focus on"),
    sectors: Optional[List[str]] = Query(None, description="Sectors to focus on")
):
    """
    Export data formatted for report writing
    
    Args:
        target_date: Date in YYYY-MM-DD format
        tickers: Optional list of tickers to focus on
        sectors: Optional list of sectors to focus on
        
    Returns:
        Report-ready data with sources and insights
    """
    try:
        # Validate date format
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
        
        master_service = get_master_service()
        result = master_service.export_report_data(target_date, tickers, sectors)
        
        if not result["success"]:
            raise HTTPException(status_code=404, detail=result["error"])
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to export report data for {target_date}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to export report data")


@router.get("/search/{target_date}")
async def search_master_data(
    target_date: str,
    q: str = Query(..., description="Search query"),
    search_type: str = Query("all", description="Search type: all, tickers, sectors, titles"),
    limit: int = Query(50, ge=1, le=200, description="Max results")
):
    """
    Search within master data
    
    Args:
        target_date: Date in YYYY-MM-DD format
        q: Search query
        search_type: Type of search to perform
        limit: Maximum number of results
        
    Returns:
        Search results with matching articles
    """
    try:
        # Validate date format
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
        
        master_service = get_master_service()
        
        # Get all data for the date
        result = master_service.query_master_data(target_date, {"include_full_content": True})
        
        if not result["success"]:
            raise HTTPException(status_code=404, detail=result["error"])
        
        articles = result["articles"]
        query_lower = q.lower()
        matched_articles = []
        
        for article in articles:
            match_score = 0
            match_reasons = []
            
            # Search in titles
            if search_type in ["all", "titles"]:
                if query_lower in article["source"]["title"].lower():
                    match_score += 10
                    match_reasons.append("title")
            
            # Search in tickers
            if search_type in ["all", "tickers"]:
                for ticker in article["quick_access"]["tickers"]:
                    if query_lower in ticker.lower():
                        match_score += 8
                        match_reasons.append("ticker")
                        break
            
            # Search in sectors
            if search_type in ["all", "sectors"]:
                for sector in article["quick_access"]["sectors"]:
                    if query_lower in sector.lower():
                        match_score += 6
                        match_reasons.append("sector")
                        break
            
            # Search in content
            if search_type == "all":
                if query_lower in article["content"]["full_text"].lower():
                    match_score += 3
                    match_reasons.append("content")
                
                if query_lower in article["content"]["description_text"].lower():
                    match_score += 5
                    match_reasons.append("description")
            
            # Include if matched
            if match_score > 0:
                article_copy = article.copy()
                article_copy["search_score"] = match_score
                article_copy["match_reasons"] = match_reasons
                
                # Truncate content for search results
                if not article_copy.get("include_full_content", False):
                    article_copy["content"]["full_text"] = article_copy["content"]["full_text"][:300] + "..." if len(article_copy["content"]["full_text"]) > 300 else article_copy["content"]["full_text"]
                
                matched_articles.append(article_copy)
        
        # Sort by match score (highest first)
        matched_articles.sort(key=lambda x: x["search_score"], reverse=True)
        
        # Limit results
        matched_articles = matched_articles[:limit]
        
        return {
            "success": True,
            "date": target_date,
            "search_query": q,
            "search_type": search_type,
            "total_matches": len(matched_articles),
            "articles": matched_articles
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to search master data for {target_date}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to search master data")


@router.get("/insights/{target_date}")
async def get_market_insights(target_date: str):
    """
    Get comprehensive market insights for a date
    
    Args:
        target_date: Date in YYYY-MM-DD format
        
    Returns:
        Market insights with trends and analysis
    """
    try:
        # Validate date format
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
        
        master_service = get_master_service()
        
        # Get full data
        result = master_service.query_master_data(target_date, {"include_full_content": False})
        
        if not result["success"]:
            raise HTTPException(status_code=404, detail=result["error"])
        
        summary = result["summary"]
        metadata = result["metadata"]
        
        # Generate insights
        insights = {
            "market_health": {
                "total_articles": metadata["total_articles"],
                "success_rate": round((metadata["successful_extractions"] / metadata["total_articles"]) * 100, 2) if metadata["total_articles"] > 0 else 0,
                "avg_confidence": round(sum([a["quick_access"]["confidence_score"] for a in result["articles"]]) / len(result["articles"]), 3) if result["articles"] else 0
            },
            "sentiment_analysis": summary["sentiment_overview"],
            "market_impact": summary["market_impact"],
            "trending_stocks": summary["top_stocks"][:5],  # Top 5
            "trending_sectors": summary["top_sectors"][:5],  # Top 5
            "financial_activity": summary["financial_metrics"],
            "key_insights": [
                f"Phân tích {metadata['total_articles']} bài báo trong ngày {target_date}",
                f"Tỷ lệ thành công: {(metadata['successful_extractions'] / metadata['total_articles']) * 100:.1f}%",
                f"Tâm lý thị trường: {max(summary['sentiment_overview'], key=lambda x: summary['sentiment_overview'][x]['count']) if summary['sentiment_overview'] else 'Trung lập'}",
                f"{summary['market_impact']['market_moving_articles']} bài báo có khả năng làm thị trường biến động"
            ]
        }
        
        return {
            "success": True,
            "date": target_date,
            "insights": insights,
            "summary": summary,
            "metadata": metadata
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get market insights for {target_date}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get market insights")


__all__ = ["router"]