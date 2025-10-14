from abc import ABC, abstractmethod
from typing import Dict, Any

class WorkflowOrchestrator(ABC):
    """Interface for orchestrating the complete workflow"""
    
    @abstractmethod
    async def run_workflow(self, workspace: str, script_path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Run the complete workflow with given payload"""
        pass


class DatabaseService(ABC):
    """Interface for database operations"""
    
    @abstractmethod
    async def connect(self) -> None:
        """Establish a connection to the database"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the connection to the database"""
        pass

