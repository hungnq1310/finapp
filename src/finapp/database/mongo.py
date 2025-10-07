"""
MongoDB Database Implementation for Financial News Analysis

This module provides MongoDB-specific implementations of the data repository.
"""

import logging
from typing import Dict, Any, Optional, List
from finapp.database.abstract import DataRepository
from finapp.schema.base import (
    RawDocument, NewsArticle, StockReport, SectorReport, MarketReport
)


class MongoDataRepository(DataRepository):
    """MongoDB implementation of data repository"""
    
    def __init__(self, database):
        self.db = database
        self.logger = logging.getLogger(__name__)
    
    def save(self, document: Any) -> str:
        """Save document to appropriate collection"""
        try:
            collection_name = self._get_collection_name(document)
            collection = self.db[collection_name]
            
            doc_dict = document.to_dict()
            collection.insert_one(doc_dict)
            
            self.logger.info(f"Saved document {document.id} to {collection_name}")
            return document.id
            
        except Exception as e:
            self.logger.error(f"Error saving document: {e}")
            raise
    
    def find_by_id(self, doc_id: str, doc_type: type) -> Optional[Any]:
        """Find document by ID"""
        try:
            collection_name = self._get_collection_name_by_type(doc_type)
            collection = self.db[collection_name]
            
            doc_dict = collection.find_one({"_id": doc_id})
            if doc_dict:
                return self._dict_to_object(doc_dict, doc_type)
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding document {doc_id}: {e}")
            return None
    
    def find_by_criteria(self, criteria: Dict[str, Any], doc_type: type) -> List[Any]:
        """Find documents by criteria"""
        try:
            collection_name = self._get_collection_name_by_type(doc_type)
            collection = self.db[collection_name]
            
            docs = list(collection.find(criteria))
            return [self._dict_to_object(doc, doc_type) for doc in docs]
            
        except Exception as e:
            self.logger.error(f"Error finding documents: {e}")
            return []
    
    def update(self, doc_id: str, updates: Dict[str, Any], doc_type: type) -> bool:
        """Update document"""
        try:
            collection_name = self._get_collection_name_by_type(doc_type)
            collection = self.db[collection_name]
            
            result = collection.update_one({"_id": doc_id}, {"$set": updates})
            return result.modified_count > 0
            
        except Exception as e:
            self.logger.error(f"Error updating document {doc_id}: {e}")
            return False
    
    def delete(self, doc_id: str, doc_type: type) -> bool:
        """Delete document"""
        try:
            collection_name = self._get_collection_name_by_type(doc_type)
            collection = self.db[collection_name]
            
            result = collection.delete_one({"_id": doc_id})
            return result.deleted_count > 0
            
        except Exception as e:
            self.logger.error(f"Error deleting document {doc_id}: {e}")
            return False
    
    def _get_collection_name(self, document: Any) -> str:
        """Get collection name from document type"""
        type_map = {
            RawDocument: "raw_documents",
            NewsArticle: "news_articles", 
            StockReport: "stock_reports",
            SectorReport: "sector_reports",
            MarketReport: "market_reports"
        }
        return type_map.get(type(document), "documents")
    
    def _get_collection_name_by_type(self, doc_type: type) -> str:
        """Get collection name from type"""
        type_map = {
            RawDocument: "raw_documents",
            NewsArticle: "news_articles",
            StockReport: "stock_reports", 
            SectorReport: "sector_reports",
            MarketReport: "market_reports"
        }
        return type_map.get(doc_type, "documents")
    
    def _dict_to_object(self, doc_dict: Dict, doc_type: type) -> Any:
        """Convert dictionary back to object - simplified version"""
        # This would need proper implementation based on each type
        # For now, return a basic object with the data
        return doc_dict
