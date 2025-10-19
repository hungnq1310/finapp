"""
Factory pattern for creating configurable crawler services

This module provides factory classes for creating crawler, extractor, and storage
services with proper dependency injection of configuration dataclasses.
"""

import logging
from typing import Dict, List, Optional, Any
from pathlib import Path

from ..config.dataclasses import (
    SystemConfig, SourceConfig, StorageConfig, LLMConfig, 
    CrawlerConfig, ExtractorConfig, MasterJSONConfig,
    get_source_config_by_name, get_llm_config_by_name
)
from ..services.crawl.storage import StorageService
from ..services.extract.master_json_service import MasterJSONService
from ..services.extract.extrator_agent import LLMExtractorAgent

logger = logging.getLogger(__name__)


class StorageServiceFactory:
    """Factory for creating storage services with different configurations"""
    
    @staticmethod
    def create(storage_config: StorageConfig, source_config: SourceConfig) -> StorageService:
        """Create storage service with full configuration injection"""
        return StorageService(storage_config, source_config)
    
    @staticmethod
    def create_for_source(source_name: str, storage_config: Optional[StorageConfig] = None) -> StorageService:
        """Create storage service for a specific source"""
        source_config = get_source_config_by_name(source_name)
        
        if storage_config is None:
            storage_config = StorageConfig()
        
        return StorageServiceFactory.create(storage_config, source_config)
    
    @staticmethod
    def create_multi_source(system_config: SystemConfig) -> Dict[str, StorageService]:
        """Create storage services for all enabled sources"""
        storage_services = {}
        
        for source_name in system_config.enabled_sources:
            try:
                storage_service = StorageServiceFactory.create_for_source(
                    source_name, system_config.crawler.storage
                )
                storage_services[source_name] = storage_service
                logger.info(f"✅ Created storage service for source: {source_name}")
            except Exception as e:
                logger.error(f"❌ Failed to create storage service for {source_name}: {e}")
                continue
        
        return storage_services


class CrawlerServiceFactory:
    """Factory for creating crawler services with different configurations"""
    
    @staticmethod
    def create_vietstock_service(storage_service: StorageService, source_config: SourceConfig) -> Any:
        """Create Vietstock-specific crawler service"""
        try:
            from ..services.crawl.vietstock_service import VietstockCrawlerService
            
            # Pass storage service and source config
            service = VietstockCrawlerService(
                base_dir=str(storage_service.output_dir.parent),
                source_name=source_config.name,
                storage_service=storage_service
            )
            
            # Apply source-specific settings
            service.base_url = source_config.base_url
            service.base_domain = source_config.base_domain
            
            return service
            
        except ImportError as e:
            logger.error(f"❌ Failed to import VietstockCrawlerService: {e}")
            raise
    
    @staticmethod
    def create_cafef_service(storage_service: StorageService, source_config: SourceConfig) -> Any:
        """Create CafeF-specific crawler service"""
        try:
            from ..services.crawl.cafef_service import CafeFCrawlerService
            
            service = CafeFCrawlerService(
                base_dir=str(storage_service.output_dir.parent),
                source_name=source_config.name,
                storage_service=storage_service
            )
            
            service.base_url = source_config.base_url
            service.base_domain = source_config.base_domain
            
            return service
            
        except ImportError:
            logger.warning("⚠️ CafeFCrawlerService not implemented yet")
            return None
    
    @staticmethod
    def create_generic_rss_service(storage_service: StorageService, source_config: SourceConfig) -> Any:
        """Create generic RSS crawler service for other sources"""
        try:
            from ..services.crawl.generic_rss_service import GenericRSSCrawlerService
            
            service = GenericRSSCrawlerService(
                base_dir=str(storage_service.output_dir.parent),
                source_name=source_config.name,
                rss_url=source_config.rss_url,
                storage_service=storage_service
            )
            
            return service
            
        except ImportError:
            logger.warning("⚠️ GenericRSSCrawlerService not implemented yet")
            return None
    
    @staticmethod
    def create_service_for_source(source_name: str, storage_service: StorageService) -> Any:
        """Create appropriate crawler service based on source name"""
        source_config = get_source_config_by_name(source_name)
        
        if source_name == "vietstock":
            return CrawlerServiceFactory.create_vietstock_service(storage_service, source_config)
        elif source_name == "cafef":
            return CrawlerServiceFactory.create_cafef_service(storage_service, source_config)
        elif source_name in ["vietnamfinance", "tuoitre", "vnexpress"]:
            return CrawlerServiceFactory.create_generic_rss_service(storage_service, source_config)
        else:
            # Default to generic RSS service
            return CrawlerServiceFactory.create_generic_rss_service(storage_service, source_config)
    
    @staticmethod
    def create_multi_source_services(system_config: SystemConfig) -> Dict[str, Any]:
        """Create crawler services for all enabled sources"""
        crawler_services = {}
        
        # Create storage services first
        storage_services = StorageServiceFactory.create_multi_source(system_config)
        
        for source_name in system_config.enabled_sources:
            if source_name in storage_services:
                try:
                    crawler_service = CrawlerServiceFactory.create_service_for_source(
                        source_name, storage_services[source_name]
                    )
                    
                    if crawler_service:
                        crawler_services[source_name] = crawler_service
                        logger.info(f"✅ Created crawler service for source: {source_name}")
                    else:
                        logger.warning(f"⚠️ No crawler service available for: {source_name}")
                        
                except Exception as e:
                    logger.error(f"❌ Failed to create crawler service for {source_name}: {e}")
                    continue
        
        return crawler_services


class ExtractorServiceFactory:
    """Factory for creating extraction services with different configurations"""
    
    @staticmethod
    def create_llm_agent(llm_config: Optional[LLMConfig] = None) -> LLMExtractorAgent:
        """Create LLM extractor agent with configuration"""
        if llm_config is None:
            llm_config = get_llm_config_by_name("custom")
        
        # Create agent with configuration injection
        agent = LLMExtractorAgent(
            api_key=llm_config.api_key,
            model_name=llm_config.model_name,
            temperature=llm_config.temperature,
            max_tokens=llm_config.max_tokens,
            base_url=llm_config.base_url
        )
        
        logger.info(f"✅ Created LLM agent with model: {llm_config.model_name}")
        return agent
    
    @staticmethod
    def create_master_json_service(master_config: Optional[MasterJSONConfig] = None) -> MasterJSONService:
        """Create master JSON service with configuration"""
        if master_config is None:
            master_config = MasterJSONConfig()
        
        service = MasterJSONService(base_dir=master_config.base_dir)
        
        logger.info(f"✅ Created MasterJSON service with base_dir: {master_config.base_dir}")
        return service
    
    @staticmethod
    def create_extraction_service(extractor_config: ExtractorConfig) -> Any:
        """Create complete extraction service with all dependencies"""
        try:
            from ..services.extract.extraction_service import ExtractionService
            
            # Create LLM agent
            llm_agent = ExtractorServiceFactory.create_llm_agent(extractor_config.llm)
            
            # Create master JSON service
            master_service = ExtractorServiceFactory.create_master_json_service(extractor_config.master_json)
            
            # Create extraction service
            service = ExtractionService()
            
            # Inject dependencies (assuming these are settable)
            if hasattr(service, 'extractor'):
                service.extractor = llm_agent
            
            # Configure service settings
            if hasattr(service, 'batch_size'):
                service.batch_size = extractor_config.llm.batch_size
            
            if hasattr(service, 'delay_seconds'):
                service.delay_seconds = extractor_config.llm.delay_seconds
            
            logger.info("✅ Created complete extraction service with all dependencies")
            return service
            
        except Exception as e:
            logger.error(f"❌ Failed to create extraction service: {e}")
            raise


class SystemServiceFactory:
    """Main factory for creating complete system services"""
    
    @staticmethod
    def create_from_config(system_config: SystemConfig) -> Dict[str, Any]:
        """Create all services from system configuration"""
        services = {
            "config": system_config,
            "storage_services": {},
            "crawler_services": {},
            "extractor_services": {}
        }
        
        try:
            # Create storage services
            services["storage_services"] = StorageServiceFactory.create_multi_source(system_config)
            
            # Create crawler services
            services["crawler_services"] = CrawlerServiceFactory.create_multi_source_services(system_config)
            
            # Create extractor services
            services["extractor_services"]["llm_agent"] = ExtractorServiceFactory.create_llm_agent(system_config.extractor.llm)
            services["extractor_services"]["master_json"] = ExtractorServiceFactory.create_master_json_service(system_config.extractor.master_json)
            services["extractor_services"]["extraction_service"] = ExtractorServiceFactory.create_extraction_service(system_config.extractor)
            
            logger.info("✅ All services created successfully from system configuration")
            return services
            
        except Exception as e:
            logger.error(f"❌ Failed to create services from config: {e}")
            raise
    
    @staticmethod
    def create_multi_source_system() -> Dict[str, Any]:
        """Create system with multiple sources"""
        from ..config.dataclasses import create_multi_source_config
        
        system_config = create_multi_source_config()
        return SystemServiceFactory.create_from_config(system_config)
    
    @staticmethod
    def create_single_source_system(source_name: str = "vietstock") -> Dict[str, Any]:
        """Create system with single source"""
        from ..config.dataclasses import create_default_config
        
        system_config = create_default_config()
        system_config.enabled_sources = [source_name]
        system_config.default_source = source_name
        
        return SystemServiceFactory.create_from_config(system_config)


class ConfigManager:
    """Manager for different configuration scenarios"""
    
    @staticmethod
    def get_config_for_scenario(scenario: str) -> SystemConfig:
        """Get configuration for different scenarios"""
        from ..config.dataclasses import create_default_config, create_multi_source_config, create_config_from_env
        
        if scenario == "development":
            return create_default_config()
        elif scenario == "production":
            return create_multi_source_config()
        elif scenario == "testing":
            config = create_default_config()
            config.crawler.enable_scheduler = False
            config.crawler.interval_minutes = 5
            config.extractor.llm.max_tokens = 1000  # Reduce for testing
            return config
        elif scenario == "cafef_only":
            config = create_default_config()
            config.enabled_sources = ["cafef"]
            config.default_source = "cafef"
            return config
        elif scenario == "all_sources":
            return create_multi_source_config()
        else:
            # Try environment-based config
            return create_config_from_env()
    
    @staticmethod
    def override_config_from_env(config: SystemConfig) -> SystemConfig:
        """Override configuration with environment variables"""
        import os
        
        # Override LLM settings
        if os.getenv("LLM_API_KEY"):
            config.extractor.llm.api_key = os.getenv("LLM_API_KEY")
        if os.getenv("LLM_MODEL_NAME"):
            config.extractor.llm.model_name = os.getenv("LLM_MODEL_NAME")
        if os.getenv("LLM_BASE_URL"):
            config.extractor.llm.base_url = os.getenv("LLM_BASE_URL")
        
        # Override storage settings
        if os.getenv("MONGODB_URI"):
            config.crawler.storage.mongodb_uri = os.getenv("MONGODB_URI")
        if os.getenv("DATABASE_NAME"):
            config.crawler.storage.database_name = os.getenv("DATABASE_NAME")
        
        # Override API settings
        if os.getenv("API_HOST"):
            config.api.host = os.getenv("API_HOST")
        if os.getenv("API_PORT"):
            config.api.port = int(os.getenv("API_PORT"))
        
        # Override crawler settings
        if os.getenv("CRAWLER_INTERVAL_MINUTES"):
            config.crawler.interval_minutes = int(os.getenv("CRAWLER_INTERVAL_MINUTES"))
        
        if os.getenv("ENABLED_SOURCES"):
            sources = os.getenv("ENABLED_SOURCES").split(",")
            config.enabled_sources = [s.strip() for s in sources]
        
        return config


# Convenience functions for backward compatibility
def create_default_vietstock_system() -> Dict[str, Any]:
    """Create default Vietstock system for backward compatibility"""
    return SystemServiceFactory.create_single_source_system("vietstock")


def create_multi_source_crawler_system() -> Dict[str, Any]:
    """Create multi-source crawler system"""
    return SystemServiceFactory.create_multi_source_system()


def create_system_for_sources(source_names: List[str]) -> Dict[str, Any]:
    """Create system for specific list of sources"""
    from ..config.dataclasses import create_default_config
    
    config = create_default_config()
    config.enabled_sources = source_names
    config.default_source = source_names[0] if source_names else "vietstock"
    
    return SystemServiceFactory.create_from_config(config)