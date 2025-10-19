"""
API Router for System Configuration Management

This module provides REST API endpoints for managing system configurations,
creating services with different configurations, and switching between sources.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

from ...config.dataclasses import (
    SystemConfig, SourceConfig, LLMConfig, StorageConfig,
    get_source_config_by_name, get_llm_config_by_name,
    create_default_config, create_multi_source_config,
    VIETSTOCK_CONFIG, CAFEF_CONFIG, CUSTOM_LLM_CONFIG
)
from ...services.factory import (
    SystemServiceFactory, ConfigManager,
    create_default_vietstock_system, create_multi_source_crawler_system
)

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/system", tags=["System Configuration"])

# Global system services
_current_system: Optional[Dict[str, Any]] = None
_current_config: Optional[SystemConfig] = None


# Request Models
class ConfigOverrideRequest(BaseModel):
    """Request model for overriding configuration"""
    scenario: Optional[str] = Field(None, description="Configuration scenario: development, production, testing, cafef_only, all_sources")
    enabled_sources: Optional[List[str]] = Field(None, description="List of enabled sources")
    llm_config: Optional[Dict[str, Any]] = Field(None, description="LLM configuration overrides")
    storage_config: Optional[Dict[str, Any]] = Field(None, description="Storage configuration overrides")
    api_config: Optional[Dict[str, Any]] = Field(None, description="API configuration overrides")


class CreateServiceRequest(BaseModel):
    """Request model for creating services with custom configuration"""
    service_type: str = Field(..., description="Type of service: crawler, extractor, storage")
    source_name: Optional[str] = Field(None, description="Source name for source-specific services")
    config: Dict[str, Any] = Field(..., description="Service configuration")


class SourceSwitchRequest(BaseModel):
    """Request model for switching active sources"""
    enabled_sources: List[str] = Field(..., description="List of sources to enable")
    default_source: str = Field(..., description="Default source to use")


# Response Models
class SystemInfoResponse(BaseModel):
    """Response model for system information"""
    current_config: Dict[str, Any]
    available_sources: List[Dict[str, Any]]
    available_llms: List[Dict[str, Any]]
    system_status: Dict[str, Any]


class ServiceCreationResponse(BaseModel):
    """Response model for service creation"""
    success: bool
    service_type: str
    service_info: Dict[str, Any]
    endpoints: List[str]


# Configuration endpoints
@router.get("/info", response_model=SystemInfoResponse)
async def get_system_info():
    """
    Get current system configuration and available options
    
    Returns:
        System information with current config and available options
    """
    try:
        global _current_config, _current_system
        
        # Get current configuration
        current_config = _current_config or create_default_config()
        
        # Available sources
        available_sources = [
            {
                "name": "vietstock",
                "display_name": "Vietstock",
                "description": "Vietnamese financial news portal",
                "base_url": VIETSTOCK_CONFIG.base_url,
                "specialization": "Vietnamese financial markets"
            },
            {
                "name": "cafef", 
                "display_name": "CafeF",
                "description": "Vietnamese financial news and market data",
                "base_url": CAFEF_CONFIG.base_url,
                "specialization": "Market data and financial news"
            },
            {
                "name": "vietnamfinance",
                "display_name": "Vietnam Finance",
                "description": "Vietnamese financial and economic news", 
                "base_url": "https://vietnamfinance.vn/rss",
                "specialization": "Economic policy and finance"
            }
        ]
        
        # Available LLMs
        available_llms = [
            {
                "name": "custom",
                "display_name": "Custom GPT-4 Nano",
                "description": "Custom API endpoint with GPT-4.1-nano",
                "model": CUSTOM_LLM_CONFIG.model_name,
                "provider": "custom"
            },
            {
                "name": "openrouter-claude",
                "display_name": "Claude 3.5 Sonnet",
                "description": "Anthropic Claude via OpenRouter",
                "model": "anthropic/claude-3.5-sonnet",
                "provider": "openrouter"
            }
        ]
        
        # System status
        system_status = {
            "services_initialized": _current_system is not None,
            "active_sources": current_config.enabled_sources,
            "default_source": current_config.default_source,
            "storage_type": current_config.crawler.storage.storage_type,
            "llm_model": current_config.extractor.llm.model_name,
            "scheduler_enabled": current_config.crawler.enable_scheduler
        }
        
        return SystemInfoResponse(
            current_config={
                "environment": current_config.environment,
                "enabled_sources": current_config.enabled_sources,
                "default_source": current_config.default_source,
                "storage": {
                    "type": current_config.crawler.storage.storage_type,
                    "database": current_config.crawler.storage.database_name
                },
                "llm": {
                    "model": current_config.extractor.llm.model_name,
                    "provider": current_config.extractor.llm.provider,
                    "temperature": current_config.extractor.llm.temperature
                }
            },
            available_sources=available_sources,
            available_llms=available_llms,
            system_status=system_status
        )
        
    except Exception as e:
        logger.error(f"❌ Failed to get system info: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get system info: {e}")


@router.post("/config/override")
async def override_system_config(request: ConfigOverrideRequest):
    """
    Override system configuration with new settings
    
    Args:
        request: Configuration override request
        
    Returns:
        Updated system configuration
    """
    try:
        global _current_config, _current_system
        
        # Get base configuration
        if request.scenario:
            _current_config = ConfigManager.get_config_for_scenario(request.scenario)
        else:
            _current_config = create_default_config()
        
        # Override enabled sources
        if request.enabled_sources:
            _current_config.enabled_sources = request.enabled_sources
            _current_config.default_source = request.enabled_sources[0]
        
        # Override LLM configuration
        if request.llm_config:
            for key, value in request.llm_config.items():
                if hasattr(_current_config.extractor.llm, key):
                    setattr(_current_config.extractor.llm, key, value)
        
        # Override storage configuration  
        if request.storage_config:
            for key, value in request.storage_config.items():
                if hasattr(_current_config.crawler.storage, key):
                    setattr(_current_config.crawler.storage, key, value)
        
        # Override API configuration
        if request.api_config:
            for key, value in request.api_config.items():
                if hasattr(_current_config.api, key):
                    setattr(_current_config.api, key, value)
        
        # Apply environment overrides
        _current_config = ConfigManager.override_config_from_env(_current_config)
        
        logger.info(f"✅ System configuration overridden")
        logger.info(f"   Enabled sources: {_current_config.enabled_sources}")
        logger.info(f"   LLM model: {_current_config.extractor.llm.model_name}")
        logger.info(f"   Storage: {_current_config.crawler.storage.storage_type}")
        
        return {
            "success": True,
            "message": "System configuration updated successfully",
            "config": {
                "enabled_sources": _current_config.enabled_sources,
                "default_source": _current_config.default_source,
                "llm_model": _current_config.extractor.llm.model_name,
                "storage_type": _current_config.crawler.storage.storage_type,
                "environment": _current_config.environment
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to override system config: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to override system config: {e}")


@router.post("/services/recreate")
async def recreate_system_services(background_tasks: BackgroundTasks):
    """
    Recreate all system services with current configuration
    
    Args:
        background_tasks: FastAPI background tasks
        
    Returns:
        Service recreation results
    """
    try:
        global _current_config, _current_system
        
        if _current_config is None:
            _current_config = create_default_config()
        
        def recreate_services():
            try:
                logger.info("🔄 Recreating system services...")
                _current_system = SystemServiceFactory.create_from_config(_current_config)
                logger.info("✅ System services recreated successfully")
            except Exception as e:
                logger.error(f"❌ Failed to recreate services: {e}")
        
        background_tasks.add_task(recreate_services)
        
        return {
            "success": True,
            "message": "Service recreation started in background",
            "config_preview": {
                "enabled_sources": _current_config.enabled_sources,
                "services_to_create": [
                    "storage_services",
                    "crawler_services", 
                    "extractor_services"
                ]
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to start service recreation: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start service recreation")


@router.post("/sources/switch", response_model=Dict[str, Any])
async def switch_active_sources(request: SourceSwitchRequest):
    """
    Switch active sources for the system
    
    Args:
        request: Source switch request
        
    Returns:
        Updated sources configuration
    """
    try:
        global _current_config, _current_system
        
        # Validate sources
        available_sources = ["vietstock", "cafef", "vietnamfinance", "tuoitre", "vnexpress"]
        invalid_sources = [s for s in request.enabled_sources if s not in available_sources]
        
        if invalid_sources:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid sources: {invalid_sources}. Available: {available_sources}"
            )
        
        if request.default_source not in request.enabled_sources:
            raise HTTPException(
                status_code=400,
                detail=f"Default source '{request.default_source}' not in enabled sources"
            )
        
        # Update configuration
        if _current_config is None:
            _current_config = create_default_config()
        
        _current_config.enabled_sources = request.enabled_sources
        _current_config.default_source = request.default_source
        
        logger.info(f"🔄 Switched to sources: {request.enabled_sources}")
        logger.info(f"📌 Default source: {request.default_source}")
        
        return {
            "success": True,
            "message": f"Switched to {len(request.enabled_sources)} sources successfully",
            "previous_sources": _current_config.enabled_sources,
            "current_sources": request.enabled_sources,
            "default_source": request.default_source,
            "note": "Recreate services to apply changes: POST /system/services/recreate"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to switch sources: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to switch sources")


@router.get("/scenarios")
async def get_available_scenarios():
    """
    Get list of available configuration scenarios
    
    Returns:
        Available scenarios with descriptions
    """
    try:
        scenarios = [
            {
                "name": "development",
                "display_name": "Development",
                "description": "Single source (Vietstock) with verbose logging",
                "enabled_sources": ["vietstock"],
                "features": ["debug_mode", "json_logging", "single_source"]
            },
            {
                "name": "production", 
                "display_name": "Production",
                "description": "Multi-source with optimized settings",
                "enabled_sources": ["vietstock", "cafef", "vietnamfinance", "tuoitre"],
                "features": ["multi_source", "hybrid_storage", "error_logging"]
            },
            {
                "name": "testing",
                "display_name": "Testing",
                "description": "Fast configuration for testing",
                "enabled_sources": ["vietstock"],
                "features": ["reduced_tokens", "fast_intervals", "no_scheduler"]
            },
            {
                "name": "cafef_only",
                "display_name": "CafeF Only",
                "description": "Single source focusing on CafeF",
                "enabled_sources": ["cafef"],
                "features": ["market_data_focus", "real_time_updates"]
            },
            {
                "name": "all_sources",
                "display_name": "All Sources",
                "description": "Maximum coverage with all available sources",
                "enabled_sources": ["vietstock", "cafef", "vietnamfinance", "tuoitre", "vnexpress"],
                "features": ["maximum_coverage", "cross_validation", "comprehensive_analysis"]
            }
        ]
        
        return {
            "success": True,
            "scenarios": scenarios,
            "current_scenario": "custom" if _current_config else "not_set"
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to get scenarios: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get scenarios")


@router.post("/llm/switch")
async def switch_llm_provider(llm_name: str = Query(..., description="LLM provider name")):
    """
    Switch LLM provider for extraction
    
    Args:
        llm_name: Name of LLM provider to switch to
        
    Returns:
        Updated LLM configuration
    """
    try:
        global _current_config
        
        # Get LLM configuration
        llm_config = get_llm_config_by_name(llm_name)
        
        if _current_config is None:
            _current_config = create_default_config()
        
        # Update LLM configuration
        _current_config.extractor.llm = llm_config
        
        logger.info(f"🔄 Switched LLM provider to: {llm_name}")
        logger.info(f"📝 Model: {llm_config.model_name}")
        logger.info(f"🔗 Base URL: {llm_config.base_url}")
        
        return {
            "success": True,
            "message": f"Switched to LLM provider: {llm_name}",
            "llm_config": {
                "provider": llm_config.provider,
                "model_name": llm_config.model_name,
                "base_url": llm_config.base_url,
                "temperature": llm_config.temperature,
                "max_tokens": llm_config.max_tokens
            },
            "note": "Recreate services to apply changes: POST /system/services/recreate"
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to switch LLM provider: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to switch LLM provider")


@router.get("/services/status")
async def get_services_status():
    """
    Get status of all system services
    
    Returns:
        Services status and health information
    """
    try:
        global _current_system, _current_config
        
        if _current_system is None:
            return {
                "success": True,
                "services_initialized": False,
                "message": "Services not initialized. Use POST /system/services/recreate",
                "services": {}
            }
        
        services_status = {}
        
        # Check storage services
        if "storage_services" in _current_system:
            storage_services = _current_system["storage_services"]
            services_status["storage"] = {
                "total_services": len(storage_services),
                "active_services": list(storage_services.keys()),
                "health": "healthy"
            }
        
        # Check crawler services
        if "crawler_services" in _current_system:
            crawler_services = _current_system["crawler_services"]
            services_status["crawler"] = {
                "total_services": len(crawler_services),
                "active_services": list(crawler_services.keys()),
                "health": "healthy"
            }
        
        # Check extractor services
        if "extractor_services" in _current_system:
            extractor_services = _current_system["extractor_services"]
            services_status["extractor"] = {
                "total_services": len(extractor_services),
                "active_services": list(extractor_services.keys()),
                "health": "healthy"
            }
        
        return {
            "success": True,
            "services_initialized": True,
            "config": {
                "enabled_sources": _current_config.enabled_sources if _current_config else [],
                "environment": _current_config.environment if _current_config else "unknown"
            },
            "services": services_status,
            "system_health": "healthy"
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to get services status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get services status")


@router.post("/config/export")
async def export_current_config():
    """
    Export current system configuration
    
    Returns:
        Current configuration as JSON
    """
    try:
        global _current_config
        
        if _current_config is None:
            _current_config = create_default_config()
        
        # Convert configuration to serializable format
        config_dict = {
            "crawler": {
                "enabled_sources": _current_config.enabled_sources,
                "default_source": _current_config.default_source,
                "interval_minutes": _current_config.crawler.interval_minutes,
                "enable_scheduler": _current_config.crawler.enable_scheduler,
                "storage": {
                    "storage_type": _current_config.crawler.storage.storage_type,
                    "database_name": _current_config.crawler.storage.database_name,
                    "base_dir": _current_config.crawler.storage.base_dir
                }
            },
            "extractor": {
                "llm": {
                    "provider": _current_config.extractor.llm.provider,
                    "model_name": _current_config.extractor.llm.model_name,
                    "base_url": _current_config.extractor.llm.base_url,
                    "temperature": _current_config.extractor.llm.temperature,
                    "max_tokens": _current_config.extractor.llm.max_tokens,
                    "batch_size": _current_config.extractor.llm.batch_size,
                    "delay_seconds": _current_config.extractor.llm.delay_seconds
                },
                "master_json": {
                    "base_dir": _current_config.extractor.master_json.base_dir,
                    "auto_generate_summary": _current_config.extractor.master_json.auto_generate_summary
                }
            },
            "api": {
                "host": _current_config.api.host,
                "port": _current_config.api.port,
                "reload": _current_config.api.reload
            },
            "system": {
                "environment": _current_config.environment,
                "debug_mode": _current_config.debug_mode
            }
        }
        
        return {
            "success": True,
            "export_timestamp": datetime.now().isoformat(),
            "config": config_dict
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to export config: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to export config")


__all__ = ["router"]