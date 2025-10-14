from typing import List, Dict, Any, Optional
import logging
import datetime
from finapp.services.abstract import DatabaseService
from finapp.database.minio import MinioDataRepository

logger = logging.getLogger(__name__)

class MinioService(DatabaseService):
    """Service class for MinIO database operations"""
    
    def __init__(self):
        self.database = None

    async def connect(self) -> None:
        """Establish a connection to the MinIO database"""
        try:
            self.database = MinioDataRepository()
            logger.info("Connected to MinIO database")
        except Exception as e:
            logger.error(f"Error connecting to MinIO: {e}")
            raise

    async def disconnect(self) -> None:
        """Disconnect from the MinIO database"""
        try:
            if self.database:
                self.database = None
                logger.info("Disconnected from MinIO database")
        except Exception as e:
            logger.error(f"Error disconnecting from MinIO: {e}")
            raise

     # Utility methods for MinIO-specific operations
    def list_index_reports(self, prefix: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        """List index reports (MinIO-specific method)"""
        if not self.database:
            logger.error("Database connection not established")
            return []
        try:
            reports = self.database.list_objects(
                prefix=prefix or "stock_reports/",
                limit=limit
            )
        
            return reports
            
        except Exception as e:
            logger.error(f"Error listing stock reports: {e}")
            return []
    
    def get_index_report(self, filename: str) -> Optional[Dict[str, Any]]:
        """Get specific index report by filename (MinIO-specific method)"""
        if not self.database:
            logger.error("Database connection not established")
            return None
        try:
            report = self.database.find_by_name(filename)
            return report

        except Exception as e:
            logger.error(f"Error getting index report {filename}: {e}")
            return None

    def get_latest_index_report(self) -> Optional[Dict[str, Any]]:
        """Get latest index report (MinIO-specific method)"""
        if not self.database:
            logger.error("Database connection not established")
            return None
        try:
            objects = list(self.database.list_objects(prefix="stock_reports/", limit=10))
            
            if not objects:
                return None
            
            # Sort by last modified date
            latest_obj = max(objects, key=lambda x: x.get("last_modified") or datetime.min)
            report = self.database.find_by_name(latest_obj.get("object_name", ""))
            return report

        except Exception as e:
            logger.error(f"Error getting latest index report: {e}")
            return None

    def get_index_report_by_date(self, target_date: str) -> Optional[Dict[str, Any]]:
        """Get index report by date (MinIO-specific method)"""
        if not self.database:
            logger.error("Database connection not established")
            return None
        try:
            # List objects that match the date pattern
            objects = list(self.database.list_objects(prefix="stock_reports/", limit=100))
            for obj in objects:
                if target_date in obj.get("object_name", ""):
                    report = self.database.find_by_name(obj.get("object_name", ""))
                    return report
            return None
            
        except Exception as e:
            logger.error(f"Error getting index report by date {target_date}: {e}")
            return None