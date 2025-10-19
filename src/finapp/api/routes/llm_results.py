"""
API Router for LLM Extraction Results Query

This module provides REST API endpoints for querying LLM extraction results from MongoDB.
"""

import logging
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Query, Path
from pydantic import BaseModel, Field

from ...database.llm_extraction import LLMExtractionRepository
from ...config import Config

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/llm-results", tags=["LLM Results Query"])

# Global repository instance
_repository: Optional[LLMExtractionRepository] = None


def get_repository() -> LLMExtractionRepository:
    """Get or create repository instance"""
    global _repository
    if _repository is None:
        _repository = LLMExtractionRepository(
            mongo_uri=Config.MONGODB_URI,
            database_name=Config.DATABASE_NAME
        )
    return _repository


# Response Models
class SentimentStatsResponse(BaseModel):
    """Response model for sentiment statistics"""
    total_extractions: int
    distribution: List[Dict[str, Any]]
    breakdown: Dict[str, int]


class TickerStatsResponse(BaseModel):
    """Response model for ticker statistics"""
    total_extractions: int
    top_tickers: List[Dict[str, Any]]
    ticker_details: Dict[str, Any]


class SectorStatsResponse(BaseModel):
    """Response model for sector statistics"""
    total_extractions: int
    top_sectors: List[Dict[str, Any]]
    sector_details: Dict[str, Any]


class MarketMovingResponse(BaseModel):
    """Response model for market moving articles"""
    total_market_moving: int
    articles: List[Dict[str, Any]]


class ComprehensiveStatsResponse(BaseModel):
    """Response model for comprehensive statistics"""
    total_sentiment_extractions: int
    total_stock_extractions: int
    total_sector_extractions: int
    total_market_extractions: int
    total_financial_extractions: int
    sentiment_distribution: List[Dict[str, Any]]
    top_tickers: List[Dict[str, Any]]
    top_sectors: List[Dict[str, Any]]
    market_moving_articles: int
    articles_with_financial_data: int


# Query endpoints
@router.get("/statistics", response_model=ComprehensiveStatsResponse)
async def get_comprehensive_statistics():
    """
    Get comprehensive statistics about LLM extraction results
    
    Returns:
        Comprehensive statistics across all extraction levels
    """
    try:
        repo = get_repository()
        stats = repo.get_extraction_statistics()
        
        if "error" in stats:
            raise HTTPException(status_code=500, detail=f"Statistics error: {stats['error']}")
        
        return ComprehensiveStatsResponse(**stats)
        
    except Exception as e:
        logger.error(f"❌ Failed to get comprehensive statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get statistics: {e}")


@router.get("/by-article/{article_guid}")
async def get_extractions_by_article(
    article_guid: str = Path(..., description="Article GUID to query")
):
    """
    Get LLM extraction results for a specific article
    
    Args:
        article_guid: Unique article identifier
        
    Returns:
        Complete LLM extraction results for the article
    """
    try:
        repo = get_repository()
        extraction = repo.get_extraction_by_guid(article_guid)
        
        if not extraction or extraction == {"article_guid": article_guid}:
            raise HTTPException(status_code=404, detail=f"No extraction found for article: {article_guid}")
        
        return {
            "success": True,
            "message": "Extraction results retrieved successfully",
            "data": extraction
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to get extraction by article GUID {article_guid}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get extraction results: {e}")


@router.get("/by-ticker/{ticker}")
async def get_extractions_by_ticker(
    ticker: str = Path(..., description="Stock ticker symbol"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
):
    """
    Get LLM extraction results for a specific ticker
    
    Args:
        ticker: Stock ticker symbol (e.g., "SJC", "FPT", "VCB")
        limit: Maximum number of results
        
    Returns:
        List of extraction results for the ticker
    """
    try:
        repo = get_repository()
        extractions = repo.get_extractions_by_ticker(ticker.upper(), limit)
        
        if not extractions:
            return {
                "success": True,
                "message": f"No extractions found for ticker: {ticker}",
                "data": {
                    "ticker": ticker.upper(),
                    "total_extractions": 0,
                    "results": []
                }
            }
        
        return {
            "success": True,
            "message": f"Retrieved {len(extractions)} extractions for {ticker}",
            "data": {
                "ticker": ticker.upper(),
                "total_extractions": len(extractions),
                "results": extractions
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to get extractions by ticker {ticker}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get extractions by ticker: {e}")


@router.get("/by-sentiment/{sentiment}")
async def get_extractions_by_sentiment(
    sentiment: str = Path(..., description="Sentiment type (tích cực/tiêu cực/trung lập)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
):
    """
    Get LLM extraction results by sentiment
    
    Args:
        sentiment: Sentiment type in Vietnamese
        limit: Maximum number of results
        
    Returns:
        List of extraction results with the specified sentiment
    """
    try:
        repo = get_repository()
        extractions = repo.get_extractions_by_sentiment(sentiment, limit)
        
        if not extractions:
            return {
                "success": True,
                "message": f"No extractions found with sentiment: {sentiment}",
                "data": {
                    "sentiment": sentiment,
                    "total_extractions": 0,
                    "results": []
                }
            }
        
        return {
            "success": True,
            "message": f"Retrieved {len(extractions)} extractions with {sentiment} sentiment",
            "data": {
                "sentiment": sentiment,
                "total_extractions": len(extractions),
                "results": extractions
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to get extractions by sentiment {sentiment}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get extractions by sentiment: {e}")


@router.get("/by-sector/{sector}")
async def get_extractions_by_sector(
    sector: str = Path(..., description="Sector name"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
):
    """
    Get LLM extraction results by sector
    
    Args:
        sector: Sector name
        limit: Maximum number of results
        
    Returns:
        List of extraction results for the sector
    """
    try:
        repo = get_repository()
        extractions = repo.get_extractions_by_sector(sector, limit)
        
        if not extractions:
            return {
                "success": True,
                "message": f"No extractions found for sector: {sector}",
                "data": {
                    "sector": sector,
                    "total_extractions": 0,
                    "results": []
                }
            }
        
        return {
            "success": True,
            "message": f"Retrieved {len(extractions)} extractions for sector {sector}",
            "data": {
                "sector": sector,
                "total_extractions": len(extractions),
                "results": extractions
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to get extractions by sector {sector}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get extractions by sector: {e}")


@router.get("/market-moving")
async def get_market_moving_articles(
    limit: int = Query(50, ge=1, le=500, description="Maximum number of results")
):
    """
    Get articles that are identified as market moving
    
    Args:
        limit: Maximum number of results
        
    Returns:
        List of market moving articles with their LLM analysis
    """
    try:
        repo = get_repository()
        articles = repo.get_market_moving_articles(limit)
        
        if not articles:
            return {
                "success": True,
                "message": "No market moving articles found",
                "data": {
                    "total_articles": 0,
                    "results": []
                }
            }
        
        return {
            "success": True,
            "message": f"Retrieved {len(articles)} market moving articles",
            "data": {
                "total_articles": len(articles),
                "results": articles
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to get market moving articles: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get market moving articles: {e}")


@router.get("/top-tickers")
async def get_top_tickers(
    limit: int = Query(20, ge=1, le=100, description="Maximum number of tickers")
):
    """
    Get most frequently mentioned tickers
    
    Args:
        limit: Maximum number of tickers
        
    Returns:
        List of top tickers with their mention counts
    """
    try:
        repo = get_repository()
        stats = repo.get_extraction_statistics()
        
        if "top_tickers" in stats:
            top_tickers = stats["top_tickers"][:limit]
            return {
                "success": True,
                "message": f"Retrieved top {len(top_tickers)} tickers",
                "data": {
                    "total_tickers": len(stats["top_tickers"]),
                    "top_tickers": top_tickers
                }
            }
        else:
            return {
                "success": True,
                "message": "No ticker data available",
                "data": {
                    "total_tickers": 0,
                    "top_tickers": []
                }
            }
        
    except Exception as e:
        logger.error(f"❌ Failed to get top tickers: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get top tickers: {e}")


@router.get("/top-sectors")
async def get_top_sectors(
    limit: int = Query(20, ge=1, le=100, description="Maximum number of sectors")
):
    """
    Get most frequently mentioned sectors
    
    Args:
        limit: Maximum number of sectors
        
    Returns:
        List of top sectors with their mention counts
    """
    try:
        repo = get_repository()
        stats = repo.get_extraction_statistics()
        
        if "top_sectors" in stats:
            top_sectors = stats["top_sectors"][:limit]
            return {
                "success": True,
                "message": f"Retrieved top {len(top_sectors)} sectors",
                "data": {
                    "total_sectors": len(stats["top_sectors"]),
                    "top_sectors": top_sectors
                }
            }
        else:
            return {
                "success": True,
                "message": "No sector data available",
                "data": {
                    "total_sectors": 0,
                    "top_sectors": []
                }
            }
        
    except Exception as e:
        logger.error(f"❌ Failed to get top sectors: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get top sectors: {e}")


@router.get("/sentiment-breakdown")
async def get_sentiment_breakdown():
    """
    Get detailed sentiment analysis breakdown
    
    Returns:
        Sentiment distribution across all extractions
    """
    try:
        repo = get_repository()
        stats = repo.get_extraction_statistics()
        
        if "sentiment_distribution" in stats:
            total_extractions = stats.get("total_sentiment_extractions", 0)
            
            # Convert to more detailed breakdown
            breakdown = {}
            for sentiment_data in stats["sentiment_distribution"]:
                sentiment = sentiment_data["_id"]
                count = sentiment_data["count"]
                percentage = (count / total_extractions * 100) if total_extractions > 0 else 0
                breakdown[sentiment] = {
                    "count": count,
                    "percentage": round(percentage, 2)
                }
            
            return {
                "success": True,
                "message": "Retrieved sentiment breakdown",
                "data": {
                    "total_extractions": total_extractions,
                    "distribution": stats["sentiment_distribution"],
                    "breakdown": breakdown
                }
            }
        else:
            return {
                "success": True,
                "message": "No sentiment data available",
                "data": {
                    "total_extractions": 0,
                    "distribution": [],
                    "breakdown": {}
                }
            }
        
    except Exception as e:
        logger.error(f"❌ Failed to get sentiment breakdown: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get sentiment breakdown: {e}")


@router.get("/search")
async def search_extractions(
    ticker: Optional[str] = Query(None, description="Filter by ticker"),
    sentiment: Optional[str] = Query(None, description="Filter by sentiment"),
    sector: Optional[str] = Query(None, description="Filter by sector"),
    market_moving: Optional[bool] = Query(None, description="Filter by market moving articles"),
    has_financial_data: Optional[bool] = Query(None, description="Filter by articles with financial data"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
):
    """
    Search LLM extraction results with multiple filters
    
    Args:
        ticker: Filter by ticker symbol
        sentiment: Filter by sentiment
        sector: Filter by sector
        market_moving: Filter by market moving articles
        has_financial_data: Filter by articles with financial data
        limit: Maximum number of results
        
    Returns:
        Filtered extraction results
    """
    try:
        repo = get_repository()
        results = []
        
        # Start with base query and apply filters
        if ticker:
            ticker_results = repo.get_extractions_by_ticker(ticker.upper(), limit)
            results.extend(ticker_results)
        
        elif sentiment:
            sentiment_results = repo.get_extractions_by_sentiment(sentiment, limit)
            results.extend(sentiment_results)
        
        elif sector:
            sector_results = repo.get_extractions_by_sector(sector, limit)
            results.extend(sector_results)
        
        elif market_moving:
            market_results = repo.get_market_moving_articles(limit)
            results.extend(market_results)
        
        else:
            # Default: return recent statistics
            return await get_comprehensive_statistics()
        
        # Apply additional filters
        if has_financial_data is not None:
            filtered_results = []
            for result in results:
                financial_data = result.get("financial_data", {})
                if financial_data.get("has_numbers", False) == has_financial_data:
                    filtered_results.append(result)
            results = filtered_results
        
        # Remove duplicates by article_guid
        seen_guids = set()
        unique_results = []
        for result in results:
            article_guid = result.get("article_guid")
            if article_guid and article_guid not in seen_guids:
                seen_guids.add(article_guid)
                unique_results.append(result)
        
        return {
            "success": True,
            "message": f"Search completed with {len(unique_results)} results",
            "data": {
                "total_results": len(unique_results),
                "filters_applied": {
                    "ticker": ticker,
                    "sentiment": sentiment,
                    "sector": sector,
                    "market_moving": market_moving,
                    "has_financial_data": has_financial_data
                },
                "results": unique_results[:limit]
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Search failed: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {e}")


__all__ = ["router"]