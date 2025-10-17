"""
Windmill Service Integration for Financial News Analysis

This module provides service classes for integrating with Windmill workflows.
"""

import uuid
import logging
import httpx
from typing import Dict, Any
from finapp.services.abstract import WorkflowOrchestrator

logger = logging.getLogger(__name__)

class WindmillService(WorkflowOrchestrator):
    """Service for Windmill workflow integration"""
    
    def __init__(self, base_url: str, token: str = ""):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.session = httpx.AsyncClient(timeout=30.0)
    
    async def health_check(self) -> Dict[str, Any]:
        """Check Windmill service health"""
        try:
            if self.token:
                self.session.headers.update({"Authorization": f"Bearer {self.token}"})
            response = await self.session.get(f"{self.base_url}/api/version")
            if response.status_code == 200:
                return {
                    "status": "healthy",
                    "url": self.base_url,
                    "version": response.json().get("version", "unknown")
                }
            else:
                return {
                    "status": "error",
                    "error": f"HTTP {response.status_code}"
                }
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    async def run_workflow(
        self, 
        workspace: str, 
        script_path: str,
        token: str,
        payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Trigger Windmill workflow"""
        try:
            url = f"{self.base_url}/api/w/{workspace}/jobs/run/f/{script_path}"

            headers = {"Content-Type": "application/json"}
            # each webhook have its own token
            if token:
                headers["Authorization"] = f"Bearer {token}"
            
            # Add correlation ID for tracking
            correlation_id = str(uuid.uuid4())
            payload["correlation_id"] = correlation_id
            
            response = await self.session.post(url, json=payload, headers=headers)
            if response.status_code in [200, 201]:
                result = response.json()
                return {
                    "success": True,
                    "workflow_id": result.get("id", ""),
                    "correlation_id": correlation_id,
                    "status": result.get("status", "running")
                }
            else:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}",
                    "message": response.text
                }
                
        except Exception as e:
            logger.error(f"Windmill workflow trigger error: {e}")
            return {"success": False, "error": str(e)}
    
    async def close(self):
        """Close the HTTP session"""
        await self.session.aclose()
