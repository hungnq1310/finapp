"""
Financial Reports API endpoints for generating comprehensive reports using longdoc system.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date, timedelta
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ...database.llm_extraction import LLMExtractionRepository
from ...database.mongo import get_mongo_client

logger = logging.getLogger(__name__)
router = APIRouter()


class AvailableEntitiesResponse(BaseModel):
    """Response model for available sectors and tickers"""
    available_sectors: List[Dict[str, Any]]
    available_tickers: List[Dict[str, Any]]
    date_range: Dict[str, str]
    total_articles: int


class ReportRequest(BaseModel):
    """Request model for generating financial reports"""
    entity_type: str  # 'sector' or 'ticker' or 'market'
    entity_value: Optional[str] = None  # sector name or ticker code
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    include_sentiment: bool = True
    include_stock_analysis: bool = True
    include_sector_analysis: bool = True
    include_market_analysis: bool = True


class FinancialReportService:
    """Service for generating financial reports using longdoc system"""
    
    def __init__(self):
        self.extraction_repo = LLMExtractionRepository()
        self.mongo_client = get_mongo_client()
        self.db = self.mongo_client["finapp"]
    
    def get_available_entities(self, days_back: int = 30) -> AvailableEntitiesResponse:
        """
        Get available industry sectors and stock tickers from LLM extraction data.
        
        Args:
            days_back: Number of days to look back for data
            
        Returns:
            AvailableEntitiesResponse with sectors, tickers, and metadata
        """
        try:
            logger.info(f"Getting available entities from last {days_back} days")
            
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # Get collections with their data
            collections_to_check = [
                "llm_sentiment_analysis",
                "llm_stock_analysis", 
                "llm_sector_analysis",
                "llm_market_analysis"
            ]
            
            all_sectors = {}  # sector_name -> count
            all_tickers = {}  # ticker -> count
            total_articles = 0
            
            for collection_name in collections_to_check:
                try:
                    collection = self.db[collection_name]
                    
                    # Query documents within date range
                    query = {
                        "created_at": {
                            "$gte": start_date.isoformat(),
                            "$lte": end_date.isoformat()
                        }
                    }
                    
                    docs = list(collection.find(query, {
                        "sectors": 1,
                        "mentioned_stocks": 1,
                        "ticker": 1,
                        "created_at": 1
                    }).limit(1000))  # Limit for performance
                    
                    logger.info(f"Found {len(docs)} documents in {collection_name}")
                    
                    for doc in docs:
                        # Extract sectors
                        if "sectors" in doc and doc["sectors"]:
                            for sector in doc["sectors"]:
                                if isinstance(sector, str):
                                    all_sectors[sector] = all_sectors.get(sector, 0) + 1
                        
                        # Extract tickers from stock analysis
                        if "ticker" in doc and doc["ticker"]:
                            ticker = doc["ticker"]
                            if isinstance(ticker, str):
                                all_tickers[ticker] = all_tickers.get(ticker, 0) + 1
                        
                        # Extract mentioned stocks
                        if "mentioned_stocks" in doc and doc["mentioned_stocks"]:
                            for stock in doc["mentioned_stocks"]:
                                if isinstance(stock, dict) and "ticker" in stock:
                                    ticker = stock["ticker"]
                                    if isinstance(ticker, str):
                                        all_tickers[ticker] = all_tickers.get(ticker, 0) + 1
                                elif isinstance(stock, str):
                                    all_tickers[stock] = all_tickers.get(stock, 0) + 1
                    
                    total_articles += len(docs)
                    
                except Exception as e:
                    logger.warning(f"Error querying collection {collection_name}: {e}")
                    continue
            
            # Convert to response format
            available_sectors = [
                {
                    "name": sector,
                    "article_count": count,
                    "last_updated": datetime.now().isoformat()
                }
                for sector, count in sorted(all_sectors.items(), key=lambda x: x[1], reverse=True)
            ]
            
            available_tickers = [
                {
                    "ticker": ticker,
                    "article_count": count,
                    "last_updated": datetime.now().isoformat()
                }
                for ticker, count in sorted(all_tickers.items(), key=lambda x: x[1], reverse=True)
            ]
            
            return AvailableEntitiesResponse(
                available_sectors=available_sectors,
                available_tickers=available_tickers,
                date_range={
                    "from": start_date.strftime("%Y-%m-%d"),
                    "to": end_date.strftime("%Y-%m-%d")
                },
                total_articles=total_articles
            )
            
        except Exception as e:
            logger.error(f"Error getting available entities: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to get available entities: {str(e)}")
    
    def gather_articles_for_report(
        self, 
        entity_type: str, 
        entity_value: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        include_sentiment: bool = True,
        include_stock_analysis: bool = True,
        include_sector_analysis: bool = True,
        include_market_analysis: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Gather relevant articles for financial report generation.
        
        Args:
            entity_type: 'sector', 'ticker', or 'market'
            entity_value: sector name or ticker code (not required for 'market')
            date_from: Start date in YYYY-MM-DD format
            date_to: End date in YYYY-MM-DD format
            include_sentiment: Include sentiment analysis data
            include_stock_analysis: Include stock analysis data
            include_sector_analysis: Include sector analysis data
            include_market_analysis: Include market analysis data
            
        Returns:
            List of relevant articles with title and content
        """
        try:
            logger.info(f"Gathering articles for {entity_type}:{entity_value}")
            
            # Parse dates
            if date_from:
                start_date = datetime.strptime(date_from, "%Y-%m-%d")
            else:
                start_date = datetime.now() - timedelta(days=7)
                
            if date_to:
                end_date = datetime.strptime(date_to, "%Y-%m-%d")
            else:
                end_date = datetime.now()
            
            # Build query based on entity type
            query = {
                "created_at": {
                    "$gte": start_date.isoformat(),
                    "$lte": end_date.isoformat()
                }
            }
            
            if entity_type == "sector" and entity_value:
                query["sectors"] = entity_value
            elif entity_type == "ticker" and entity_value:
                query["$or"] = [
                    {"ticker": entity_value},
                    {"mentioned_stocks.ticker": entity_value}
                ]
            # For 'market' type, no additional filter needed
            
            # Gather from relevant collections
            collections_to_search = []
            if include_sentiment:
                collections_to_search.append("llm_sentiment_analysis")
            if include_stock_analysis:
                collections_to_search.append("llm_stock_analysis")
            if include_sector_analysis:
                collections_to_search.append("llm_sector_analysis")
            if include_market_analysis:
                collections_to_search.append("llm_market_analysis")
            
            all_articles = []
            
            for collection_name in collections_to_search:
                try:
                    collection = self.db[collection_name]
                    
                    # Query documents
                    docs = list(collection.find(query).limit(500))  # Limit for performance
                    
                    for doc in docs:
                        # Extract article information
                        article = {
                            "id": str(doc.get("_id", "")),
                            "title": doc.get("title", "Untitled"),
                            "content": self._extract_content_from_doc(doc),
                            "source_collection": collection_name,
                            "created_at": doc.get("created_at", ""),
                            "entity_type": entity_type,
                            "entity_value": entity_value,
                            "metadata": {
                                "sectors": doc.get("sectors", []),
                                "ticker": doc.get("ticker", ""),
                                "mentioned_stocks": doc.get("mentioned_stocks", []),
                                "sentiment": doc.get("overall_sentiment", ""),
                                "confidence": doc.get("confidence", 0.0)
                            }
                        }
                        
                        # Only include articles with meaningful content
                        if article["content"] and len(article["content"].strip()) > 100:
                            all_articles.append(article)
                    
                    logger.info(f"Found {len(docs)} documents in {collection_name}")
                    
                except Exception as e:
                    logger.warning(f"Error querying collection {collection_name}: {e}")
                    continue
            
            logger.info(f"Gathered {len(all_articles)} total articles for report")
            return all_articles
            
        except Exception as e:
            logger.error(f"Error gathering articles for report: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to gather articles: {str(e)}")
    
    def _extract_content_from_doc(self, doc: Dict[str, Any]) -> str:
        """Extract meaningful content from a document."""
        content_parts = []
        
        # Try different content fields
        if "summary" in doc and doc["summary"]:
            content_parts.append(doc["summary"])
        
        if "analysis" in doc and doc["analysis"]:
            if isinstance(doc["analysis"], str):
                content_parts.append(doc["analysis"])
            elif isinstance(doc["analysis"], dict):
                # Extract key points from analysis dict
                for key, value in doc["analysis"].items():
                    if isinstance(value, str) and len(value.strip()) > 20:
                        content_parts.append(f"{key}: {value}")
        
        if "key_points" in doc and doc["key_points"]:
            if isinstance(doc["key_points"], list):
                content_parts.extend([str(point) for point in doc["key_points"] if str(point).strip()])
            else:
                content_parts.append(str(doc["key_points"]))
        
        if "market_impact" in doc and doc["market_impact"]:
            content_parts.append(f"Market Impact: {doc['market_impact']}")
        
        if "recommendations" in doc and doc["recommendations"]:
            content_parts.append(f"Recommendations: {doc['recommendations']}")
        
        # Join content parts
        content = "\n\n".join(content_parts)
        
        # If still no meaningful content, try to use the title
        if not content or len(content.strip()) < 50:
            title = doc.get("title", "")
            if title:
                content = f"Article Title: {title}\n\n[Full content not available in extraction database]"
            else:
                content = "[Content not available]"
        
        return content.strip()


# Initialize service
financial_service = FinancialReportService()


@router.get("/available-entities", response_model=AvailableEntitiesResponse)
async def get_available_entities(
    days_back: int = Query(default=30, ge=1, le=365, description="Number of days to look back")
):
    """
    Get available industry sectors and stock tickers for report generation.
    
    Args:
        days_back: Number of days to look back for data (1-365)
        
    Returns:
        Available sectors, tickers, and metadata
    """
    try:
        return financial_service.get_available_entities(days_back)
    except Exception as e:
        logger.error(f"Error in get_available_entities: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gather-articles")
async def gather_articles_for_report(
    request: ReportRequest
):
    """
    Gather relevant articles for financial report generation.
    
    This endpoint collects articles related to specific sectors, tickers, or market analysis
    from the LLM extraction database for use with the longdoc system.
    """
    try:
        # Validate request
        if request.entity_type not in ["sector", "ticker", "market"]:
            raise HTTPException(status_code=400, detail="entity_type must be 'sector', 'ticker', or 'market'")
        
        if request.entity_type in ["sector", "ticker"] and not request.entity_value:
            raise HTTPException(status_code=400, detail=f"entity_value is required for entity_type '{request.entity_type}'")
        
        # Gather articles
        articles = financial_service.gather_articles_for_report(
            entity_type=request.entity_type,
            entity_value=request.entity_value,
            date_from=request.date_from,
            date_to=request.date_to,
            include_sentiment=request.include_sentiment,
            include_stock_analysis=request.include_stock_analysis,
            include_sector_analysis=request.include_sector_analysis,
            include_market_analysis=request.include_market_analysis
        )
        
        if not articles:
            return {
                "message": "No articles found for the specified criteria",
                "articles": [],
                "total_count": 0,
                "criteria": request.dict()
            }
        
        # Create document for longdoc processing
        document_content = financial_service._create_document_for_longdoc(
            articles, request.entity_type, request.entity_value
        )
        
        return {
            "message": f"Found {len(articles)} relevant articles",
            "articles": articles[:20],  # Return first 20 for preview
            "total_count": len(articles),
            "criteria": request.dict(),
            "document_for_longdoc": {
                "title": f"{request.entity_type.title()} Analysis: {request.entity_value or 'Market Overview'}",
                "content": document_content,
                "metadata": {
                    "article_count": len(articles),
                    "entity_type": request.entity_type,
                    "entity_value": request.entity_value,
                    "generated_at": datetime.now().isoformat()
                }
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in gather_articles_for_report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Add method to FinancialReportService
def _create_document_for_longdoc(self, articles: List[Dict[str, Any]], entity_type: str, entity_value: Optional[str]) -> str:
    """Create a comprehensive document from articles for longdoc processing."""
    if not articles:
        return "No articles available."
    
    content_parts = []
    
    # Add header
    header = f"# {entity_type.title()} Analysis Report: {entity_value or 'Market Overview'}\n\n"
    header += f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    header += f"Total Articles Analyzed: {len(articles)}\n\n"
    content_parts.append(header)
    
    # Add articles grouped by collection/source
    by_collection = {}
    for article in articles:
        collection = article["source_collection"]
        if collection not in by_collection:
            by_collection[collection] = []
        by_collection[collection].append(article)
    
    for collection, collection_articles in by_collection.items():
        content_parts.append(f"## {collection.replace('_', ' ').title()}\n")
        
        for i, article in enumerate(collection_articles, 1):
            content_parts.append(f"### Article {i}: {article['title']}\n")
            
            # Add metadata
            metadata = article["metadata"]
            if metadata.get("sectors"):
                content_parts.append(f"**Sectors:** {', '.join(metadata['sectors'])}\n")
            if metadata.get("ticker"):
                content_parts.append(f"**Ticker:** {metadata['ticker']}\n")
            if metadata.get("mentioned_stocks"):
                stocks = [s.get('ticker', s) if isinstance(s, dict) else str(s) for s in metadata['mentioned_stocks']]
                content_parts.append(f"**Mentioned Stocks:** {', '.join(stocks)}\n")
            if metadata.get("sentiment"):
                content_parts.append(f"**Sentiment:** {metadata['sentiment']}\n")
            
            content_parts.append(f"**Date:** {article['created_at']}\n\n")
            
            # Add content
            content_parts.append(f"{article['content']}\n\n")
            content_parts.append("---\n\n")
    
    return "\n".join(content_parts)


# Add the method to the class
FinancialReportService._create_document_for_longdoc = _create_document_for_longdoc


# Import the financial report generator
from ...services.financial_reports import FinancialReportGenerator

# Initialize the generator
try:
    report_generator = FinancialReportGenerator()
except Exception as e:
    logger.error(f"Failed to initialize report generator: {e}")
    report_generator = None


@router.post("/generate-report")
async def generate_financial_report(
    request: ReportRequest
):
    """
    Generate a comprehensive financial report using the longdoc system.
    
    This endpoint uses the longdoc module to create structured financial reports
    with RAG-based content generation.
    """
    if not report_generator:
        raise HTTPException(status_code=503, detail="Financial report generator is not available")
    
    try:
        # Validate request
        if request.entity_type not in ["sector", "ticker", "market"]:
            raise HTTPException(status_code=400, detail="entity_type must be 'sector', 'ticker', or 'market'")
        
        if request.entity_type in ["sector", "ticker"] and not request.entity_value:
            raise HTTPException(status_code=400, detail=f"entity_value is required for entity_type '{request.entity_type}'")
        
        # Generate report
        result = report_generator.generate_financial_report(
            entity_type=request.entity_type,
            entity_value=request.entity_value,
            date_from=request.date_from,
            date_to=request.date_to,
            include_sentiment=request.include_sentiment,
            include_stock_analysis=request.include_stock_analysis,
            include_sector_analysis=request.include_sector_analysis,
            include_market_analysis=request.include_market_analysis
        )
        
        if result["success"]:
            return {
                "success": True,
                "message": result["message"],
                "report": result["report"],
                "metadata": result["metadata"]
            }
        else:
            raise HTTPException(status_code=404, detail=result["message"])
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in generate_financial_report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check():
    """Health check for the financial report system."""
    if not report_generator:
        return {
            "status": "unavailable",
            "error": "Financial report generator is not initialized"
        }
    
    try:
        health = report_generator.health_check()
        return health
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "error",
            "error": str(e)
        }