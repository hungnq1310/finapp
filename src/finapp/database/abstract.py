from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class DataRepository(ABC):
    """Abstract base class for data access operations"""
    
    @abstractmethod
    def save(self, document: Any) -> str:
        """Save document and return its ID"""
        pass
    
    @abstractmethod
    def find_by_id(self, doc_id: str, doc_type: type) -> Optional[Any]:
        """Find document by ID"""
        pass
     
    @abstractmethod
    def find_by_criteria(self, criteria: Dict[str, Any], doc_type: type) -> List[Any]:
        """Find documents matching criteria"""
        pass
    
    @abstractmethod
    def update(self, doc_id: str, updates: Dict[str, Any], doc_type: type) -> bool:
        """Update document by ID"""
        pass
    
    @abstractmethod
    def delete(self, doc_id: str, doc_type: type) -> bool:
        """Delete document by ID"""
        pass