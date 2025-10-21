"""
Vietstock-specific MongoDB repository

This module provides MongoDB repository implementations specifically for Vietstock data.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from .abstract import DataRepository
from ..schema.vietstock import VietstockArticle, VietstockCrawlSession


logger = logging.getLogger(__name__)


class VietstockRepository(DataRepository):
    """MongoDB repository for Vietstock articles and crawl sessions"""
    
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017", 
                 database_name: str = "financial_news"):
        """
        Initialize Vietstock repository
        
        Args:
            mongo_uri: MongoDB connection URI
            database_name: Name of the database
        """
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.client = None
        self.db = None
        self._connect()
    
    def _connect(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.database_name]
            
            # Test connection
            self.client.admin.command('ping')
            logger.info(f"Connected to MongoDB at {self.mongo_uri}")
            
            # Create indexes for better performance
            self._create_indexes()
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def _create_indexes(self):
        """Create MongoDB indexes for better performance and duplicate prevention"""
        try:
            # Articles collection indexes
            articles_collection = self.db.vietstock_articles
            
            # Primary unique index for duplicate prevention
            articles_collection.create_index(
                "content.rss_guid", 
                unique=True, 
                sparse=True,
                name="idx_unique_guid"
            )
            
            # Performance indexes for queries
            articles_collection.create_index([("published_at", -1)], name="idx_published_at_desc")
            articles_collection.create_index([("rss_category", 1), ("published_at", -1)], name="idx_category_published")
            articles_collection.create_index([("created_at", -1)], name="idx_created_at_desc")
            articles_collection.create_index([("source.url", 1)], name="idx_source_url")
            
            # Content search indexes (for future search functionality)
            articles_collection.create_index([
                ("content.headline", "text"), 
                ("content.summary", "text"),
                ("content.body", "text")
            ], name="idx_content_search")
            
            # Crawl sessions collection indexes
            sessions_collection = self.db.vietstock_crawl_sessions
            sessions_collection.create_index([("created_at", -1)], name="idx_sessions_created")
            
            # Try to create success index with fallback handling
            try:
                sessions_collection.create_index("success", name="idx_sessions_success")
            except Exception as e:
                if "Index already exists" in str(e):
                    logger.info("Success index already exists, checking if it's properly configured")
                    # Drop old auto-generated index if it exists and create new one
                    try:
                        sessions_collection.drop_index("success_1")
                        sessions_collection.create_index("success", name="idx_sessions_success")
                        logger.info("Successfully replaced auto-generated success index")
                    except Exception:
                        logger.info("Using existing success index (may be auto-generated)")
                else:
                    logger.warning(f"Could not create success index: {e}")
            
            logger.info("MongoDB indexes created successfully")
            logger.info("   - Unique index on RSS GUID for duplicate prevention")
            logger.info("   - Performance indexes for common queries")
            logger.info("   - Text search indexes for content")
            
        except Exception as e:
            logger.warning(f"Could not create indexes: {e}")
    
    def save_article(self, article: VietstockArticle) -> bool:
        """
        Save Vietstock article to MongoDB
        
        Args:
            article: Vietstock article to save
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            collection = self.db.vietstock_articles
            article_dict = article.to_dict()
            
            # Use upsert to handle duplicates gracefully
            result = collection.replace_one(
                {"content.rss_guid": article.get_rss_guid()},
                article_dict,
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"Created new article: {article.id}")
            else:
                logger.debug(f"Updated existing article: {article.id}")

            return True
            
        except DuplicateKeyError:
            logger.debug(f"Article already exists: {article.get_rss_guid()}")
            return False
        except Exception as e:
            logger.error(f"Error saving article {article.id}: {e}")
            return False
    
    def save_articles_batch(self, articles: List[VietstockArticle]) -> Dict[str, int]:
        """
        Save multiple articles in batch
        
        Args:
            articles: List of articles to save
            
        Returns:
            Dictionary with success and failure counts
        """
        results = {"success": 0, "failed": 0, "duplicates": 0}
        
        if not articles:
            return results
        
        try:
            collection = self.db.vietstock_articles
            
            for article in articles:
                try:
                    article_dict = article.to_dict()
                    
                    # Try to insert, handle duplicate if exists
                    result = collection.replace_one(
                        {"content.rss_guid": article.get_rss_guid()},
                        article_dict,
                        upsert=True
                    )
                    
                    if result.upserted_id:
                        results["success"] += 1
                    else:
                        results["duplicates"] += 1
                        
                except Exception as e:
                    logger.debug(f"Failed to save article {article.id}: {e}")
                    results["failed"] += 1
            
            logger.info(f"Batch save results: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error in batch save: {e}")
            results["failed"] = len(articles)
            return results
    
    def find_article_by_guid(self, guid: str) -> Optional[VietstockArticle]:
        """
        Find article by RSS GUID
        
        Args:
            guid: RSS GUID to search for
            
        Returns:
            Vietstock article if found, None otherwise
        """
        try:
            collection = self.db.vietstock_articles
            doc = collection.find_one({"content.rss_guid": guid})
            
            if doc:
                return self._dict_to_vietstock_article(doc)
            return None
            
        except Exception as e:
            logger.error(f"Error finding article by GUID {guid}: {e}")
            return None
    
    def find_articles_by_category(self, category: str, limit: int = 100) -> List[VietstockArticle]:
        """
        Find articles by RSS category
        
        Args:
            category: RSS category to filter by
            limit: Maximum number of articles to return
            
        Returns:
            List of Vietstock articles
        """
        try:
            collection = self.db.vietstock_articles
            docs = list(collection.find(
                {"rss_category": category}
            ).sort("published_at", -1).limit(limit))
            
            return [self._dict_to_vietstock_article(doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"Error finding articles by category {category}: {e}")
            return []
    
    def find_articles_by_date_range(self, start_date: datetime, end_date: datetime, 
                                  category: Optional[str] = None) -> List[VietstockArticle]:
        """
        Find articles within a date range
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            category: Optional category filter
            
        Returns:
            List of Vietstock articles
        """
        try:
            collection = self.db.vietstock_articles
            query = {
                "published_at": {
                    "$gte": start_date.isoformat(),
                    "$lte": end_date.isoformat()
                }
            }
            
            if category:
                query["rss_category"] = category
            
            docs = list(collection.find(query).sort("published_at", -1))
            return [self._dict_to_vietstock_article(doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"Error finding articles by date range: {e}")
            return []
    
    def get_articles_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about articles
        
        Returns:
            Statistics dictionary
        """
        try:
            collection = self.db.vietstock_articles
            
            # Total articles
            total_articles = collection.count_documents({})
            
            # Articles by category
            category_pipeline = [
                {"$group": {"_id": "$rss_category", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            category_stats = list(collection.aggregate(category_pipeline))
            
            # Articles by date (last 7 days) - handle string dates
            date_pipeline = [
                {
                    "$addFields": {
                        "parsed_date": {
                            "$dateFromString": {
                                "dateString": "$published_at",
                                "onError": None
                            }
                        }
                    }
                },
                {
                    "$match": {
                        "parsed_date": {"$ne": None}
                    }
                },
                {
                    "$group": {
                        "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$parsed_date"}},
                        "count": {"$sum": 1}
                    }
                },
                {"$sort": {"_id": -1}},
                {"$limit": 7}
            ]
            date_stats = list(collection.aggregate(date_pipeline))
            
            # HTML extraction stats
            html_extracted = collection.count_documents({"content.html_extraction_success": True})
            html_extraction_rate = html_extracted / total_articles if total_articles > 0 else 0
            
            # Latest article
            latest_article = collection.find_one({}, sort=[("published_at", -1)])
            
            return {
                "total_articles": total_articles,
                "html_extraction_stats": {
                    "total_extracted": html_extracted,
                    "extraction_rate": round(html_extraction_rate * 100, 2)
                },
                "categories": [
                    {"name": stat["_id"], "count": stat["count"]} 
                    for stat in category_stats
                ],
                "daily_counts": date_stats,
                "latest_article_date": latest_article.get("published_at") if latest_article else None,
                "last_updated": datetime.now(datetime.timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {"error": str(e)}
    
    def save_crawl_session(self, session: VietstockCrawlSession) -> bool:
        """
        Save crawl session to MongoDB
        
        Args:
            session: Crawl session to save
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            collection = self.db.vietstock_crawl_sessions
            session_dict = session.to_dict()
            
            result = collection.insert_one(session_dict)
            logger.info(f"Saved crawl session: {session.id}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving crawl session {session.id}: {e}")
            return False
    
    def get_recent_crawl_sessions(self, limit: int = 10) -> List[VietstockCrawlSession]:
        """
        Get recent crawl sessions
        
        Args:
            limit: Maximum number of sessions to return
            
        Returns:
            List of crawl sessions
        """
        try:
            collection = self.db.vietstock_crawl_sessions
            docs = list(collection.find().sort("created_at", -1).limit(limit))
            
            return [self._dict_to_crawl_session(doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"Error getting recent crawl sessions: {e}")
            return []
    
    def _dict_to_vietstock_article(self, doc: Dict) -> VietstockArticle:
        """Convert MongoDB document back to VietstockArticle object"""
        # This is a simplified conversion - in production, you'd want
        # proper reconstruction of nested objects
        return doc  # Return raw dict for now
    
    def _dict_to_crawl_session(self, doc: Dict) -> VietstockCrawlSession:
        """Convert MongoDB document back to VietstockCrawlSession object"""
        # This is a simplified conversion
        return doc  # Return raw dict for now
    
    # Implement abstract methods from DataRepository
    def save(self, document: Any) -> str:
        """Generic save method"""
        if isinstance(document, VietstockArticle):
            if self.save_article(document):
                return document.id
        elif isinstance(document, VietstockCrawlSession):
            if self.save_crawl_session(document):
                return document.id
        return ""
    
    def find_by_id(self, doc_id: str, doc_type: type) -> Optional[Any]:
        """Generic find by ID method"""
        # Implementation for generic interface
        return None
    
    def find_by_criteria(self, criteria: Dict[str, Any], doc_type: type) -> List[Any]:
        """Generic find by criteria method"""
        if doc_type == VietstockArticle:
            try:
                collection = self.db.vietstock_articles
                docs = list(collection.find(criteria))
                return [self._dict_to_vietstock_article(doc) for doc in docs]
            except Exception as e:
                logger.error(f"Error finding articles by criteria: {e}")
                return []
        return []
    
    def update(self, doc_id: str, updates: Dict[str, Any], doc_type: type) -> bool:
        """Generic update method"""
        try:
            if doc_type == VietstockArticle:
                collection = self.db.vietstock_articles
                result = collection.update_one({"_id": doc_id}, {"$set": updates})
                return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error updating document {doc_id}: {e}")
        return False
    
    def delete(self, doc_id: str, doc_type: type) -> bool:
        """Generic delete method"""
        try:
            if doc_type == VietstockArticle:
                collection = self.db.vietstock_articles
                result = collection.delete_one({"_id": doc_id})
                return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error deleting document {doc_id}: {e}")
        return False
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")