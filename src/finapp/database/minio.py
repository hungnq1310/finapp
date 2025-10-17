"""
MinIO Database Implementation for Financial News Analysis

This module provides MinIO-specific implementations of the data repository.
"""
import json
import logging
import uuid
from typing import List, Optional, Dict, Any
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import os
from io import BytesIO

from finapp.database.abstract import DataRepository

logger = logging.getLogger(__name__)


class MinioDataRepository(DataRepository):
    """MinIO implementation of DataRepository for object storage"""
    
    def __init__(self):
        """Initialize MinIO client with configuration"""
        self.endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.access_key = os.getenv("MINIO_ACCESS_KEY", "")
        self.secret_key = os.getenv("MINIO_SECRET_KEY", "")
        self.bucket_name = os.getenv("MINIO_BUCKET_NAME", "")
        self.secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
        
        # Remove http:// or https:// from endpoint if present
        if self.endpoint.startswith("http://"):
            self.endpoint = self.endpoint[7:]
            self.secure = False
        elif self.endpoint.startswith("https://"):
            self.endpoint = self.endpoint[8:]
            self.secure = True
            
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure
        )
        
        # Ensure bucket exists
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Ensure the bucket exists, create if it doesn't"""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"Error ensuring bucket exists: {e}")
            raise
    
    def save(self, document: Any) -> str:
        """Save document to MinIO and return its ID"""
        try:
            # Generate object name based on document type and timestamp
            doc_id = getattr(document, 'id', None) or str(uuid.uuid4())
            doc_type_name = self._get_document_type_name(document)
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            object_name = f"{doc_type_name}/{timestamp}_{doc_id}.json"
            
            # Convert document to JSON
            if hasattr(document, '__dict__'):
                document_data = document.__dict__
            else:
                document_data = document
                
            json_data = json.dumps(document_data, default=str, ensure_ascii=False)
            
            # Upload to MinIO
            data_bytes = json_data.encode('utf-8')
            self.client.put_object(
                self.bucket_name,
                object_name,
                data=BytesIO(data_bytes),
                length=len(data_bytes),
                content_type='application/json'
            )
            
            logger.info(f"Saved document {doc_id} to {object_name}")
            return doc_id
            
        except Exception as e:
            logger.error(f"Error saving document: {e}")
            raise
    
    def find_by_id(self, doc_id: str, doc_type: type) -> Optional[Any]:
        """Find document by ID"""
        try:
            doc_type_name = self._get_document_type_name_by_type(doc_type)
            
            # List objects with prefix matching the document type
            objects = self.client.list_objects(
                self.bucket_name, 
                prefix=f"{doc_type_name}/",
                recursive=True
            )
            
            # Find object with matching ID
            for obj in objects:
                if doc_id in obj.object_name:
                    response = self.client.get_object(self.bucket_name, obj.object_name)
                    data = json.loads(response.read().decode('utf-8'))
                    return self._dict_to_object(data, doc_type)
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding document by ID {doc_id}: {e}")
            return None
    
    def find_by_criteria(self, criteria: Dict[str, Any], doc_type: type) -> List[Any]:
        """Find documents by criteria"""
        try:
            doc_type_name = self._get_document_type_name_by_type(doc_type)
            results = []
            
            # List all objects of this type
            objects = self.client.list_objects(
                self.bucket_name, 
                prefix=f"{doc_type_name}/",
                recursive=True
            )
            
            # Filter by criteria (basic implementation)
            for obj in objects:
                try:
                    response = self.client.get_object(self.bucket_name, obj.object_name)
                    data = json.loads(response.read().decode('utf-8'))
                    
                    # Check if document matches criteria
                    if self._matches_criteria(data, criteria):
                        results.append(self._dict_to_object(data, doc_type))
                        
                except Exception as e:
                    logger.warning(f"Error processing object {obj.object_name}: {e}")
                    continue
            
            return results
            
        except Exception as e:
            logger.error(f"Error finding documents by criteria: {e}")
            return []
    
    def update(self, doc_id: str, updates: Dict[str, Any], doc_type: type) -> bool:
        """Update document by ID"""
        try:
            # Find existing document
            existing_doc = self.find_by_id(doc_id, doc_type)
            if not existing_doc:
                return False
            
            # Update document data
            if hasattr(existing_doc, '__dict__'):
                doc_data = existing_doc.__dict__
            else:
                doc_data = existing_doc
                
            doc_data.update(updates)
            
            # Save updated document
            self.save(existing_doc)
            return True
            
        except Exception as e:
            logger.error(f"Error updating document {doc_id}: {e}")
            return False
    
    def delete(self, doc_id: str, doc_type: type) -> bool:
        """Delete document by ID"""
        try:
            doc_type_name = self._get_document_type_name_by_type(doc_type)
            
            # Find and delete object with matching ID
            objects = self.client.list_objects(
                self.bucket_name, 
                prefix=f"{doc_type_name}/",
                recursive=True
            )
            
            for obj in objects:
                if doc_id in obj.object_name:
                    self.client.remove_object(self.bucket_name, obj.object_name)
                    logger.info(f"Deleted document {doc_id}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error deleting document {doc_id}: {e}")
            return False
        
    def list_objects(self, prefix: str = "", limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """List objects in the bucket with optional prefix and limit"""
        try:
            objects = self.client.list_objects(
                self.bucket_name, 
                prefix=prefix,
                recursive=True, 
                **kwargs
            )
            
            result = []
            count = 0
            for obj in objects:
                if count >= limit:
                    break
                response = self.client.get_object(self.bucket_name, obj.object_name)
                data = json.loads(response.read().decode('utf-8'))
                result.append({
                    "object_name": obj.object_name,
                    "size": obj.size,
                    "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                    "data": data
                })
                count += 1
            
            return result
            
        except Exception as e:
            logger.error(f"Error listing objects: {e}")
            return []
        
    def find_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find object by name"""
        try:
            response = self.client.get_object(self.bucket_name, name)
            data = json.loads(response.read().decode('utf-8'))
            return data
            
        except Exception as e:
            logger.error(f"Error finding object by name {name}: {e}")
            return None

    def _get_document_type_name(self, document: Any) -> str:
        """Get document type name for object naming"""
        if hasattr(document, '__class__'):
            return document.__class__.__name__.lower()
        return "unknown_document"
    
    def _get_document_type_name_by_type(self, doc_type: type) -> str:
        """Get document type name from type"""
        return doc_type.__name__.lower()
    
    def _dict_to_object(self, doc_dict: Dict, doc_type: type) -> Any:
        """Convert dictionary to object of specified type"""
        try:
            # Simple implementation - you might want to enhance this
            if hasattr(doc_type, '__init__'):
                return doc_type(**doc_dict)
            return doc_dict
        except Exception as e:
            logger.warning(f"Could not convert dict to {doc_type}: {e}")
            return doc_dict
    
    def _matches_criteria(self, document: Dict[str, Any], criteria: Dict[str, Any]) -> bool:
        """Check if document matches search criteria"""
        for key, value in criteria.items():
            if key not in document or document[key] != value:
                return False
        return True
