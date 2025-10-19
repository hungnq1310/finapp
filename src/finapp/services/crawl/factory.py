"""
Crawler Factory for Multiple News Sources

This module provides a factory pattern to create crawlers for different news sources
while maintaining consistent interface and behavior.
"""

import logging
from typing import Dict, Any, Optional, List
from abc import ABC, abstractmethod

from .models import RSSCategory, Article
from .storage import StorageService
from .parser import RSSParser
from .crawler import VietstockCrawlerService

logger = logging.getLogger(__name__)


class BaseCrawlerService(ABC):
    """Abstract base class for all crawler services"""
    
    def __init__(self, base_url: str, base_dir: str, source_name: str, **kwargs):
        self.base_url = base_url
        self.base_dir = base_dir
        self.source_name = source_name
        self.storage = StorageService(
            base_dir=base_dir,
            source_name=source_name,
            **kwargs
        )
        self.parser = self._create_parser()
        
    @abstractmethod
    def _create_parser(self):
        """Create source-specific parser"""
        pass
    
    @abstractmethod
    def get_source_config(self) -> Dict[str, Any]:
        """Get source-specific configuration"""
        pass


class CrawlerFactory:
    """Factory to create crawlers for different news sources"""
    
    _crawler_registry = {}
    
    @classmethod
    def register_crawler(cls, source_name: str, crawler_class: type):
        """Register a new crawler type"""
        cls._crawler_registry[source_name] = crawler_class
        logger.info(f"✅ Registered crawler: {source_name}")
    
    @classmethod
    def create_crawler(cls, source_name: str, **kwargs) -> BaseCrawlerService:
        """Create crawler instance for the specified source"""
        if source_name not in cls._crawler_registry:
            raise ValueError(f"Unknown crawler source: {source_name}")
        
        crawler_class = cls._crawler_registry[source_name]
        return crawler_class(source_name=source_name, **kwargs)
    
    @classmethod
    def get_available_sources(cls) -> List[str]:
        """Get list of available crawler sources"""
        return list(cls._crawler_registry.keys())


# Built-in crawler implementations
class VietstockCrawler(BaseCrawlerService):
    """Vietstock crawler implementation"""
    
    def __init__(self, **kwargs):
        # Vietstock-specific defaults
        default_config = {
            'base_url': 'https://vietstock.vn/rss',
            'base_domain': 'https://vietstock.vn',
            'base_dir': 'data',
            'source_name': 'vietstock'
        }
        default_config.update(kwargs)
        
        super().__init__(**default_config)
        
        # Import existing Vietstock crawler logic
        from .crawler import VietstockCrawlerService
        self._vietstock_crawler = VietstockCrawlerService(
            base_url=default_config['base_url'],
            base_dir=default_config['base_dir'],
            source_name=default_config['source_name']
        )
    
    def _create_parser(self):
        from .parser import RSSParser
        return RSSParser(self.base_url)
    
    def get_source_config(self) -> Dict[str, Any]:
        return {
            'base_url': self.base_url,
            'base_domain': self.base_url,
            'html_extraction_delay': 2.0,
            'html_batch_size': 10,
            'supports_categories': True,
            'supports_html_extraction': True,
            'date_filtering': True,
            'vietnam_timezone': True
        }
    
    # Delegate to existing Vietstock crawler methods
    def crawl_all_categories(self, filter_by_today: bool = True):
        return self._vietstock_crawler.crawl_all_categories(filter_by_today)
    
    def crawl_with_html_extraction(self, filter_by_today: bool = True, extract_html: bool = True):
        return self._vietstock_crawler.crawl_with_html_extraction(filter_by_today, extract_html)
    
    def get_crawl_statistics(self):
        return self._vietstock_crawler.get_crawl_statistics()


class CafeF_Crawler(BaseCrawlerService):
    """CafeF crawler implementation"""
    
    def __init__(self, **kwargs):
        default_config = {
            'base_url': 'https://cafef.vn/rss',
            'base_domain': 'https://cafef.vn',
            'base_dir': 'data',
            'source_name': 'cafef'
        }
        default_config.update(kwargs)
        super().__init__(**default_config)
    
    def _create_parser(self):
        from .parser import RSSParser
        return RSSParser(self.base_domain)
    
    def get_source_config(self) -> Dict[str, Any]:
        return {
            'base_url': self.base_url,
            'base_domain': self.base_domain,
            'html_extraction_delay': 3.0,
            'html_batch_size': 5,
            'supports_categories': False,  # CafeF might have different structure
            'supports_html_extraction': True,
            'date_filtering': True,
            'vietnam_timezone': True
        }
    
    def crawl_all_categories(self, filter_by_today: bool = True):
        # Implement CafeF-specific crawling logic
        logger.info(f"🚀 Starting CafeF crawl session{' (today only)' if filter_by_today else ''}")
        # TODO: Implement CafeF parsing logic
        pass


class VietnamFinance_Crawler(BaseCrawlerService):
    """Vietnam Finance crawler implementation"""
    
    def __init__(self, **kwargs):
        default_config = {
            'base_url': 'https://vietnamfinance.vn/rss',
            'base_domain': 'https://vietnamfinance.vn',
            'base_dir': 'data',
            'source_name': 'vietnamfinance'
        }
        default_config.update(kwargs)
        super().__init__(**default_config)
    
    def _create_parser(self):
        from .parser import RSSParser
        return RSSParser(self.base_domain)
    
    def get_source_config(self) -> Dict[str, Any]:
        return {
            'base_url': self.base_url,
            'base_domain': self.base_domain,
            'html_extraction_delay': 2.5,
            'html_batch_size': 8,
            'supports_categories': True,
            'supports_html_extraction': True,
            'date_filtering': True,
            'vietnam_timezone': True
        }
    
    def crawl_all_categories(self, filter_by_today: bool = True):
        # Implement Vietnam Finance specific logic
        logger.info(f"🚀 Starting Vietnam Finance crawl session{' (today only)' if filter_by_today else ''}")
        # TODO: Implement Vietnam Finance parsing logic
        pass


# Register built-in crawlers
CrawlerFactory.register_crawler('vietstock', VietstockCrawler)
CrawlerFactory.register_crawler('cafef', CafeF_Crawler)
CrawlerFactory.register_crawler('vietnamfinance', VietnamFinance_Crawler)


class MultiSourceCrawler:
    """Manager for multiple news sources"""
    
    def __init__(self, sources: List[str] = None, **kwargs):
        self.sources = sources or ['vietstock']  # Default to Vietstock
        self.crawlers = {}
        
        # Initialize crawlers for each source
        for source in self.sources:
            try:
                self.crawlers[source] = CrawlerFactory.create_crawler(source, **kwargs)
                logger.info(f"✅ Initialized crawler for {source}")
            except Exception as e:
                logger.error(f"❌ Failed to initialize crawler for {source}: {e}")
    
    def crawl_all_sources(self, filter_by_today: bool = True, extract_html: bool = False):
        """Crawl from all configured sources"""
        results = {}
        
        for source_name, crawler in self.crawlers.items():
            try:
                logger.info(f"📡 Crawling source: {source_name}")
                
                if extract_html:
                    result = crawler.crawl_with_html_extraction(filter_by_today, True)
                else:
                    result = crawler.crawl_all_categories(filter_by_today)
                
                results[source_name] = {
                    'success': True,
                    'data': result,
                    'articles_found': result.total_articles if hasattr(result, 'total_articles') else 0
                }
                
            except Exception as e:
                logger.error(f"❌ Failed to crawl {source_name}: {e}")
                results[source_name] = {
                    'success': False,
                    'error': str(e),
                    'articles_found': 0
                }
        
        return results
    
    def get_combined_statistics(self):
        """Get combined statistics from all sources"""
        combined_stats = {
            'sources': list(self.crawlers.keys()),
            'total_articles': 0,
            'source_stats': {}
        }
        
        for source_name, crawler in self.crawlers.items():
            try:
                stats = crawler.get_crawl_statistics()
                combined_stats['source_stats'][source_name] = stats
                
                # Add to total if available
                if 'mongo_statistics' in stats and 'total_articles' in stats['mongo_statistics']:
                    combined_stats['total_articles'] += stats['mongo_statistics']['total_articles']
                    
            except Exception as e:
                logger.error(f"❌ Failed to get stats for {source_name}: {e}")
                combined_stats['source_stats'][source_name] = {'error': str(e)}
        
        return combined_stats