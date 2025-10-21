"""
MongoDB Repository for LLM Extraction Results

This module provides MongoDB repository implementations for storing LLM extraction results.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, IndexModel
from pymongo.errors import DuplicateKeyError

from .abstract import DataRepository

logger = logging.getLogger(__name__)


class LLMExtractionRepository(DataRepository):
    """MongoDB repository for LLM extraction results"""
    
    def __init__(self, mongo_uri: str = None, database_name: str = None):
        """
        Initialize LLM Extraction repository
        
        Args:
            mongo_uri: MongoDB connection URI (if None, uses Config.MONGODB_URI)
            database_name: Name of the database (if None, uses Config.DATABASE_NAME)
        """
        # Import Config here to avoid circular imports
        from src.finapp.config import Config
        
        self.mongo_uri = mongo_uri or Config.MONGODB_URI
        self.database_name = database_name or Config.DATABASE_NAME
        
        # Validate required configuration
        if not self.mongo_uri:
            error_msg = (
                "\nCONFIGURATION ERROR: MongoDB URI is required\n"
                "Please set MONGODB_URI in your .env file.\n"
                "Example: MONGODB_URI=mongodb://localhost:27017\n"
            )
            logger.error(error_msg)
            raise ValueError("MONGODB_URI is required in .env file")
        
        if not self.database_name:
            error_msg = (
                "\nCONFIGURATION ERROR: Database name is required\n"
                "Please set DATABASE_NAME in your .env file.\n"
                "Example: DATABASE_NAME=financial_news\n"
            )
            logger.error(error_msg)
            raise ValueError("DATABASE_NAME is required in .env file")
        
        self.client = None
        self.db = None
        self._connect()
    
    def _connect(self):
        """Connect to MongoDB with proper error handling"""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.database_name]
            
            # Test connection
            self.client.admin.command('ping')
            logger.info(f"LLM Extraction Repository connected to MongoDB")
            logger.info(f"   Database: {self.database_name}")
            
            # Create indexes
            self._create_indexes()
            
        except Exception as e:
            error_msg = (
                f"\nFailed to connect to MongoDB\n"
                f"URI: {self.mongo_uri[:20]}...\n"
                f"Database: {self.database_name}\n"
                f"Error: {str(e)}\n\n"
                f"Please check:\n"
                f"1. MongoDB is running\n"
                f"2. MONGODB_URI in .env is correct\n"
                f"3. Network connectivity to MongoDB server\n"
            )
            logger.error(error_msg)
            raise
    
    def save_article_content(self, article_guid: str, article_data: Dict[str, Any]) -> bool:
        """Save full article content for Longdoc processing"""
        try:
            collection = self.db.llm_article_contents
            
            # Prepare article content document
            content_doc = {
                "article_guid": article_guid,
                "title": article_data.get("article_title", ""),
                "url": article_data.get("article_url", ""),
                "content": article_data.get("article_content", ""),
                "summary": article_data.get("article_summary", ""),
                "published_date": article_data.get("published_date", ""),
                "category": article_data.get("category", ""),
                "tickers": article_data.get("extracted_tickers", []),
                "sectors": article_data.get("extracted_sectors", []),
                "market_moving": article_data.get("market_moving", False),
                "sentiment": article_data.get("overall_sentiment", ""),
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
            
            # Insert or update
            result = collection.replace_one(
                {"article_guid": article_guid},
                content_doc,
                upsert=True
            )
            
            return result.acknowledged
            
        except Exception as e:
            logger.error(f"Failed to save article content {article_guid}: {e}")
            return False
    
    def query_articles_for_report(
        self, 
        report_type: str, 
        entity_value: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Query articles for report generation"""
        try:
            collection = self.db.llm_article_contents
            
            # Build query based on report type
            query = {"created_at": {"$lte": datetime.now(timezone.utc)}}
            
            if date_from or date_to:
                date_query = {}
                if date_from:
                    date_query["$gte"] = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                if date_to:
                    date_query["$lte"] = datetime.strptime(date_to, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
                query["created_at"] = date_query
            
            if report_type == "ticker" and entity_value:
                query["$or"] = [
                    {"tickers": entity_value},
                    {"content": {"$regex": entity_value, "$options": "i"}}
                ]
            elif report_type == "sector" and entity_value:
                query["$or"] = [
                    {"sectors": entity_value},
                    {"content": {"$regex": entity_value, "$options": "i"}}
                ]
            elif report_type == "market":
                query["market_moving"] = True
            
            # Execute query
            docs = list(collection.find(query).sort("created_at", -1).limit(limit))
            
            logger.info(f"Found {len(docs)} articles for {report_type} report on {entity_value}")
            return docs
            
        except Exception as e:
            logger.error(f"Failed to query articles for report: {e}")
            return []
    
    def _create_indexes(self):
        """Create MongoDB indexes for LLM extraction collections"""
        try:
            # Add master collection for completeness tracking
            master_collection = self.db.llm_extractions
            master_collection.create_index("article_guid", unique=True, name="idx_master_guid")
            master_collection.create_index("extraction_summary.overall_sentiment", name="idx_master_sentiment")
            master_collection.create_index("extraction_summary.stocks_mentioned", name="idx_master_stocks")
            master_collection.create_index("extraction_summary.market_moving", name="idx_master_market_moving")
            
            # Sentiment Analysis collection
            sentiment_collection = self.db.llm_sentiment_analysis
            sentiment_collection.create_index("article_guid", unique=True, name="idx_sentiment_guid")
            sentiment_collection.create_index("extraction_timestamp", name="idx_sentiment_timestamp")
            sentiment_collection.create_index("overall_sentiment", name="idx_sentiment_overall")
            
            # Stock Level collection
            stock_collection = self.db.llm_stock_analysis
            stock_collection.create_index([("article_guid", 1), ("ticker", 1)], unique=True, name="idx_stock_article_ticker")
            stock_collection.create_index("ticker", name="idx_stock_ticker")
            stock_collection.create_index("sentiment", name="idx_stock_sentiment")
            stock_collection.create_index("impact_type", name="idx_stock_impact_type")
            
            # Sector Level collection
            sector_collection = self.db.llm_sector_analysis
            sector_collection.create_index([("article_guid", 1), ("sector_name", 1)], unique=True, name="idx_sector_article_sector")
            sector_collection.create_index("sector_name", name="idx_sector_name")
            sector_collection.create_index("sentiment", name="idx_sector_sentiment")
            
            # Market Level collection
            market_collection = self.db.llm_market_analysis
            market_collection.create_index("article_guid", unique=True, name="idx_market_guid")
            market_collection.create_index("scope", name="idx_market_scope")
            market_collection.create_index("exchange", name="idx_market_exchange")
            market_collection.create_index("market_moving", name="idx_market_moving")
            
            # Market Level collection
            market_collection = self.db.llm_market_analysis
            market_collection.create_index("article_guid", unique=True, name="idx_market_guid")
            market_collection.create_index("scope", name="idx_market_scope")
            market_collection.create_index("exchange", name="idx_market_exchange")
            market_collection.create_index("market_moving", name="idx_market_moving")
            market_collection.create_index("key_indices", name="idx_market_indices")
            
            # Financial Data collection
            financial_collection = self.db.llm_financial_data
            financial_collection.create_index("article_guid", unique=True, name="idx_financial_guid")
            financial_collection.create_index("has_numbers", name="idx_financial_has_numbers")
            
            # Article Content collection (for Longdoc processing)
            article_content_collection = self.db.llm_article_contents
            article_content_collection.create_index("article_guid", unique=True, name="idx_article_content_guid")
            article_content_collection.create_index([("ticker", 1), ("created_at", -1)], name="idx_article_content_ticker_time")
            article_content_collection.create_index([("sectors", 1), ("created_at", -1)], name="idx_article_content_sector_time")
            article_content_collection.create_index([("market_moving", 1), ("created_at", -1)], name="idx_article_content_market_time")
            article_content_collection.create_index("created_at", name="idx_article_content_time")
            
            # Additional indexes for efficient multi-article queries
            # For ticker-based queries (across all collections)
            stock_collection.create_index([("mentioned_stocks.ticker", 1), ("created_at", -1)], name="idx_stock_mentioned_ticker_time")
            sentiment_collection.create_index([("mentioned_stocks.ticker", 1), ("created_at", -1)], name="idx_sentiment_mentioned_ticker_time")
            sector_collection.create_index([("affected_companies", 1), ("created_at", -1)], name="idx_sector_companies_time")
            
            # For date-based queries
            for collection_name in ["llm_sentiment_analysis", "llm_stock_analysis", "llm_sector_analysis", "llm_market_analysis"]:
                collection = self.db[collection_name]
                collection.create_index([("created_at", -1)], name=f"idx_{collection_name.split('_')[-1]}_time")
            
            # Extraction Sessions collection (extended)
            extraction_sessions_collection = self.db.llm_extraction_sessions
            extraction_sessions_collection.create_index("session_id", unique=True, name="idx_extraction_session_id")
            extraction_sessions_collection.create_index("start_time", name="idx_extraction_start_time")
            extraction_sessions_collection.create_index("status", name="idx_extraction_status")
            
            logger.info("LLM Extraction MongoDB indexes created successfully")
            logger.info("   - Added master collection for completeness tracking")
            
        except Exception as e:
            logger.warning(f"Could not create LLM extraction indexes: {e}")
    
    def save_sentiment_analysis(self, article_guid: str, extraction_data: Dict[str, Any]) -> bool:
        """Save sentiment analysis result"""
        try:
            collection = self.db.llm_sentiment_analysis
            
            # Prepare document
            doc = {
                "article_guid": article_guid,
                "overall_sentiment": extraction_data.get("overall_sentiment"),
                "sentiment_score": extraction_data.get("sentiment_score"),
                "key_factors": extraction_data.get("key_factors", []),
                "extraction_timestamp": extraction_data.get("extraction_timestamp", datetime.now(timezone.utc)),
                "extraction_model": extraction_data.get("extraction_model"),
                "confidence": extraction_data.get("confidence", 0.0),
                "created_at": datetime.now(timezone.utc)
            }
            
            result = collection.replace_one(
                {"article_guid": article_guid},
                doc,
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"Created sentiment analysis: {article_guid}")
            else:
                logger.debug(f"Updated sentiment analysis: {article_guid}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving sentiment analysis {article_guid}: {e}")
            return False
    
    def save_stock_analysis(self, article_guid: str, stock_data_list: List[Dict[str, Any]]) -> bool:
        """Save stock level analysis results"""
        try:
            collection = self.db.llm_stock_analysis
            created_count = 0
            
            for stock_data in stock_data_list:
                doc = {
                    "article_guid": article_guid,
                    "ticker": stock_data.get("ticker"),
                    "company_name": stock_data.get("company_name"),
                    "sentiment": stock_data.get("sentiment"),
                    "impact_type": stock_data.get("impact_type"),
                    "price_impact": stock_data.get("price_impact"),
                    "confidence": stock_data.get("confidence", 0.0),
                    "extraction_timestamp": stock_data.get("extraction_timestamp", datetime.now(timezone.utc)),
                    "extraction_model": stock_data.get("extraction_model"),
                    "created_at": datetime.now(timezone.utc)
                }
                
                result = collection.replace_one(
                    {"article_guid": article_guid, "ticker": stock_data.get("ticker")},
                    doc,
                    upsert=True
                )
                
                if result.upserted_id:
                    created_count += 1
            
            if created_count > 0:
                logger.info(f"Created {created_count} stock analyses for: {article_guid}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving stock analysis {article_guid}: {e}")
            return False
    
    def save_sector_analysis(self, article_guid: str, sector_data_list: List[Dict[str, Any]]) -> bool:
        """Save sector level analysis results"""
        try:
            collection = self.db.llm_sector_analysis
            created_count = 0
            
            for sector_data in sector_data_list:
                doc = {
                    "article_guid": article_guid,
                    "sector_name": sector_data.get("sector_name"),
                    "sentiment": sector_data.get("sentiment"),
                    "impact_description": sector_data.get("impact_description"),
                    "affected_companies": sector_data.get("affected_companies", []),
                    "extraction_timestamp": sector_data.get("extraction_timestamp", datetime.now(timezone.utc)),
                    "extraction_model": sector_data.get("extraction_model"),
                    "created_at": datetime.now(timezone.utc)
                }
                
                result = collection.replace_one(
                    {"article_guid": article_guid, "sector_name": sector_data.get("sector_name")},
                    doc,
                    upsert=True
                )
                
                if result.upserted_id:
                    created_count += 1
            
            if created_count > 0:
                logger.info(f"Created {created_count} sector analyses for: {article_guid}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving sector analysis {article_guid}: {e}")
            return False
    
    def save_market_analysis(self, article_guid: str, market_data: Dict[str, Any]) -> bool:
        """Save market level analysis result"""
        try:
            collection = self.db.llm_market_analysis
            
            doc = {
                "article_guid": article_guid,
                "scope": market_data.get("scope"),
                "exchange": market_data.get("exchange"),
                "market_moving": market_data.get("market_moving"),
                "impact_magnitude": market_data.get("impact_magnitude"),
                "key_indices": market_data.get("key_indices", []),
                "extraction_timestamp": market_data.get("extraction_timestamp", datetime.now(timezone.utc)),
                "extraction_model": market_data.get("extraction_model"),
                "created_at": datetime.now(timezone.utc)
            }
            
            result = collection.replace_one(
                {"article_guid": article_guid},
                doc,
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"Created market analysis: {article_guid}")
            else:
                logger.debug(f"Updated market analysis: {article_guid}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving market analysis {article_guid}: {e}")
            return False
    
    def save_financial_data(self, article_guid: str, financial_data: Dict[str, Any]) -> bool:
        """Save financial data extraction result"""
        try:
            collection = self.db.llm_financial_data
            
            doc = {
                "article_guid": article_guid,
                "has_numbers": financial_data.get("has_numbers", False),
                "revenues": financial_data.get("revenues", []),
                "profits": financial_data.get("profits", []),
                "percentages": financial_data.get("percentages", []),
                "amounts": financial_data.get("amounts", []),
                "extraction_timestamp": financial_data.get("extraction_timestamp", datetime.now(timezone.utc)),
                "extraction_model": financial_data.get("extraction_model"),
                "created_at": datetime.now(timezone.utc)
            }
            
            result = collection.replace_one(
                {"article_guid": article_guid},
                doc,
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"Created financial data: {article_guid}")
            else:
                logger.debug(f"Updated financial data: {article_guid}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving financial data {article_guid}: {e}")
            return False
    
    def save_complete_extraction(self, article_guid: str, extraction_result: Dict[str, Any]) -> bool:
        """Save complete LLM extraction result (all levels) with master tracking"""
        try:
            success = True
            components_saved = []
            
            # 1. Save master record first for completeness tracking
            master_success = self._save_master_extraction(article_guid, extraction_result)
            if master_success:
                components_saved.append("master")
            
            # 2. Save sentiment analysis
            if "sentiment_analysis" in extraction_result:
                if self.save_sentiment_analysis(article_guid, extraction_result["sentiment_analysis"]):
                    components_saved.append("sentiment")
                else:
                    success = False
            
            # 3. Save stock level analysis
            if "stock_level" in extraction_result:
                if self.save_stock_analysis(article_guid, extraction_result["stock_level"]):
                    components_saved.append("stocks")
                else:
                    success = False
            
            # 4. Save sector level analysis
            if "sector_level" in extraction_result:
                if self.save_sector_analysis(article_guid, extraction_result["sector_level"]):
                    components_saved.append("sectors")
                else:
                    success = False
            
            # 5. Save market level analysis
            if "market_level" in extraction_result:
                if self.save_market_analysis(article_guid, extraction_result["market_level"]):
                    components_saved.append("market")
                else:
                    success = False
            
            # 6. Save financial data
            if "financial_data" in extraction_result:
                if self.save_financial_data(article_guid, extraction_result["financial_data"]):
                    components_saved.append("financial")
                else:
                    success = False
            
            # Update master record with completion status
            if success:
                self._update_master_completion(article_guid, components_saved, "completed")
                logger.info(f"Complete LLM extraction saved: {article_guid} (components: {', '.join(components_saved)})")
            else:
                self._update_master_completion(article_guid, components_saved, "partial")
                logger.warning(f"Partial LLM extraction saved: {article_guid} (components: {', '.join(components_saved)})")
            
            return success
            
        except Exception as e:
            logger.error(f"Error saving complete extraction {article_guid}: {e}")
            return False
    
    def _save_master_extraction(self, article_guid: str, extraction_result: Dict[str, Any]) -> bool:
        """Save master extraction record for completeness tracking"""
        try:
            collection = self.db.llm_extractions
            
            # Extract summary information
            sentiment = extraction_result.get("sentiment_analysis", {})
            stocks = extraction_result.get("stock_level", [])
            sectors = extraction_result.get("sector_level", [])
            market = extraction_result.get("market_level", {})
            financial = extraction_result.get("financial_data", {})
            
            # Create summary
            summary = {
                "overall_sentiment": sentiment.get("overall_sentiment", "trung lập"),
                "sentiment_score": sentiment.get("sentiment_score", 0.0),
                "stocks_mentioned": [stock.get("ticker", "") for stock in stocks],
                "sectors_affected": [sector.get("sector_name", "") for sector in sectors],
                "market_moving": market.get("market_moving", False),
                "has_financial_data": financial.get("has_numbers", False),
                "extraction_quality_score": extraction_result.get("extraction_confidence", 0.0)
            }
            
            # Prepare master document
            master_doc = {
                "article_guid": article_guid,
                "extraction_metadata": {
                    "extraction_timestamp": extraction_result.get("extraction_timestamp", datetime.now(timezone.utc)),
                    "extraction_model": extraction_result.get("extraction_model", "unknown"),
                    "extraction_confidence": extraction_result.get("extraction_confidence", 0.0)
                },
                "article_info": {
                    "title": extraction_result.get("article_title", ""),
                    "category": extraction_result.get("article_category", "")
                },
                "article_metadata": extraction_result.get("article_metadata", {}),  # ADD THIS!
                "extraction_summary": summary,
                "completion_status": "in_progress",
                "components_saved": [],
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
            
            # Use upsert
            result = collection.replace_one(
                {"article_guid": article_guid},
                master_doc,
                upsert=True
            )
            
            logger.debug(f"Master extraction created: {article_guid}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving master extraction {article_guid}: {e}")
            return False
    
    def _update_master_completion(self, article_guid: str, components_saved: List[str], status: str) -> bool:
        """Update master record with completion status"""
        try:
            collection = self.db.llm_extractions
            
            update_result = collection.update_one(
                {"article_guid": article_guid},
                {
                    "$set": {
                        "completion_status": status,
                        "components_saved": components_saved,
                        "updated_at": datetime.now(timezone.utc)
                    }
                }
            )
            
            return update_result.modified_count > 0
            
        except Exception as e:
            logger.error(f"Error updating master completion {article_guid}: {e}")
            return False
    
    def get_extraction_by_guid(self, article_guid: str) -> Dict[str, Any]:
        """Get complete extraction result by article GUID"""
        try:
            result = {"article_guid": article_guid}
            
            # Get sentiment analysis
            sentiment = self.db.llm_sentiment_analysis.find_one({"article_guid": article_guid})
            if sentiment:
                sentiment.pop("_id", None)
                result["sentiment_analysis"] = sentiment
            
            # Get stock analysis
            stocks = list(self.db.llm_stock_analysis.find({"article_guid": article_guid}))
            result["stock_level"] = []
            for stock in stocks:
                stock.pop("_id", None)
                result["stock_level"].append(stock)
            
            # Get sector analysis
            sectors = list(self.db.llm_sector_analysis.find({"article_guid": article_guid}))
            result["sector_level"] = []
            for sector in sectors:
                sector.pop("_id", None)
                result["sector_level"].append(sector)
            
            # Get market analysis
            market = self.db.llm_market_analysis.find_one({"article_guid": article_guid})
            if market:
                market.pop("_id", None)
                result["market_level"] = market
            
            # Get financial data
            financial = self.db.llm_financial_data.find_one({"article_guid": article_guid})
            if financial:
                financial.pop("_id", None)
                result["financial_data"] = financial
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting extraction by GUID {article_guid}: {e}")
            return {}
    
    def get_extractions_by_ticker(self, ticker: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get extractions for a specific ticker"""
        try:
            stocks = list(self.db.llm_stock_analysis.find({"ticker": ticker}).limit(limit))
            
            results = []
            for stock in stocks:
                article_guid = stock["article_guid"]
                stock.pop("_id", None)
                
                # Get complete extraction for this article
                extraction = self.get_extraction_by_guid(article_guid)
                extraction["stock_specific"] = stock
                results.append(extraction)
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting extractions by ticker {ticker}: {e}")
            return []
    
    def get_extractions_by_sentiment(self, sentiment: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get extractions by sentiment"""
        try:
            sentiment_docs = list(self.db.llm_sentiment_analysis.find({"overall_sentiment": sentiment}).limit(limit))
            
            results = []
            for doc in sentiment_docs:
                article_guid = doc["article_guid"]
                extraction = self.get_extraction_by_guid(article_guid)
                results.append(extraction)
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting extractions by sentiment {sentiment}: {e}")
            return []
    
    def get_extractions_by_sector(self, sector_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get extractions by sector"""
        try:
            sector_docs = list(self.db.llm_sector_analysis.find({"sector_name": sector_name}).limit(limit))
            
            results = []
            for doc in sector_docs:
                article_guid = doc["article_guid"]
                extraction = self.get_extraction_by_guid(article_guid)
                results.append(extraction)
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting extractions by sector {sector_name}: {e}")
            return []
    
    def get_market_moving_articles(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get articles that are market moving"""
        try:
            market_docs = list(self.db.llm_market_analysis.find({"market_moving": True}).limit(limit))
            
            results = []
            for doc in market_docs:
                article_guid = doc["article_guid"]
                extraction = self.get_extraction_by_guid(article_guid)
                results.append(extraction)
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting market moving articles: {e}")
            return []
    
    def get_extraction_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics about LLM extractions"""
        try:
            stats = {}
            
            # Master collection statistics (NEW)
            master_collection = self.db.llm_extractions
            stats["total_articles_processed"] = master_collection.count_documents({})
            stats["completed_extractions"] = master_collection.count_documents({"completion_status": "completed"})
            stats["partial_extractions"] = master_collection.count_documents({"completion_status": "partial"})
            stats["in_progress_extractions"] = master_collection.count_documents({"completion_status": "in_progress"})
            
            # Data completeness check (NEW)
            pipeline = [
                {"$group": {
                    "_id": "$completion_status",
                    "count": {"$sum": 1},
                    "avg_components": {"$avg": {"$size": "$components_saved"}}
                }}
            ]
            stats["compliance_status"] = list(master_collection.aggregate(pipeline))
            
            # Total extractions by level
            stats["total_sentiment_extractions"] = self.db.llm_sentiment_analysis.count_documents({})
            stats["total_stock_extractions"] = self.db.llm_stock_analysis.count_documents({})
            stats["total_sector_extractions"] = self.db.llm_sector_analysis.count_documents({})
            stats["total_market_extractions"] = self.db.llm_market_analysis.count_documents({})
            stats["total_financial_extractions"] = self.db.llm_financial_data.count_documents({})
            
            # Sentiment distribution
            sentiment_pipeline = [
                {"$group": {"_id": "$overall_sentiment", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            stats["sentiment_distribution"] = list(self.db.llm_sentiment_analysis.aggregate(sentiment_pipeline))
            
            # Top tickers
            ticker_pipeline = [
                {"$group": {"_id": "$ticker", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            stats["top_tickers"] = list(self.db.llm_stock_analysis.aggregate(ticker_pipeline))
            
            # Top sectors
            sector_pipeline = [
                {"$group": {"_id": "$sector_name", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            stats["top_sectors"] = list(self.db.llm_sector_analysis.aggregate(sector_pipeline))
            
            # Market moving articles
            stats["market_moving_articles"] = self.db.llm_market_analysis.count_documents({"market_moving": True})
            
            # Articles with financial data
            stats["articles_with_financial_data"] = self.db.llm_financial_data.count_documents({"has_numbers": True})
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting extraction statistics: {e}")
            return {"error": str(e)}
    
    def check_extraction_completeness(self, article_guid: str) -> Dict[str, Any]:
        """Check if extraction is complete for a specific article (NEW)"""
        try:
            master_collection = self.db.llm_extractions
            master_record = master_collection.find_one({"article_guid": article_guid})
            
            if not master_record:
                return {"exists": False, "message": "No extraction record found"}
            
            # Check all required components
            required_components = ["sentiment", "stocks", "sectors", "market", "financial"]
            saved_components = master_record.get("components_saved", [])
            missing_components = [comp for comp in required_components if comp not in saved_components]
            
            return {
                "exists": True,
                "completion_status": master_record.get("completion_status", "unknown"),
                "components_saved": saved_components,
                "missing_components": missing_components,
                "is_complete": len(missing_components) == 0,
                "quality_score": master_record.get("extraction_summary", {}).get("extraction_quality_score", 0.0)
            }
            
        except Exception as e:
            logger.error(f"Error checking extraction completeness {article_guid}: {e}")
            return {"error": str(e)}
    
    # Implement abstract methods
    def save(self, document: Any) -> str:
        """Generic save method"""
        return ""
    
    def find_by_id(self, doc_id: str, doc_type: type) -> Optional[Any]:
        """Generic find by ID method"""
        return None
    
    def find_by_criteria(self, criteria: Dict[str, Any], doc_type: type) -> List[Any]:
        """Generic find by criteria method"""
        return []
    
    def update(self, doc_id: str, updates: Dict[str, Any], doc_type: type) -> bool:
        """Generic update method"""
        return False
    
    def delete(self, doc_id: str, doc_type: type) -> bool:
        """Generic delete method"""
        return False
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("LLM Extraction Repository MongoDB connection closed")