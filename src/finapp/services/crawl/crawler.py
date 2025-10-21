"""
Vietstock Crawler Service with MongoDB Backend

This service replaces the SQLite-based crawler with MongoDB storage
while maintaining full compatibility with the existing API.
"""

import time
import logging
import os
from datetime import datetime, timezone, date
from typing import Optional, List, Dict, Any

from .models import Article, RSSCategory, CrawlSession
from .parser import RSSParser
from .storage import StorageService

logger = logging.getLogger(__name__)


class VietstockCrawlerService:
    """Vietstock crawler service with MongoDB backend"""
    
    def __init__(self, base_url: str = None, base_dir: str = "data", 
                 source_name: str = "vietstock", mongo_uri: str = None, 
                 database_name: str = None):
        # Use config values if parameters not provided
        try:
            from ...config import Config
            self.base_url = base_url or Config.CRAWLER_BASE_URL
            self.base_domain = Config.CRAWLER_BASE_DOMAIN
            self.html_extraction_delay = Config.CRAWLER_HTML_EXTRACTION_DELAY
            self.html_batch_size = Config.CRAWLER_HTML_BATCH_SIZE
        except ImportError:
            # Fallback to default values if config not available
            self.base_url = base_url or "https://vietstock.vn/rss"
            self.base_domain = "https://vietstock.vn"
            self.html_extraction_delay = 2.0
            self.html_batch_size = 10
        
        # Initialize services
        self.parser = RSSParser(self.base_domain)
        
        # Initialize storage with configuration
        from ...config.dataclasses import get_source_config_by_name, StorageConfig
        source_config = get_source_config_by_name(source_name)
        storage_config = StorageConfig(
            storage_type="hybrid",
            base_dir=base_dir,
            mongodb_uri=mongo_uri or os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
            database_name=database_name or os.getenv("DATABASE_NAME", "financial_news")
        )
        self.storage = StorageService(storage_config, source_config)
        
        # HTML extractor will be initialized on demand
        self.html_extractor = None
        
        logger.info(f"VietstockMongoCrawlerService initialized")
        logger.info(f"Base RSS URL: {self.base_url}")
        logger.info(f"MongoDB Database: {self.storage.storage_config.database_name}")
        logger.info(f"Export Directory: {self.storage.output_dir}")
        logger.info("HTML content extractor ready (lazy initialization)")
    
    def _get_html_extractor(self):
        """Get HTML extractor instance (lazy initialization)"""
        if self.html_extractor is None:
            try:
                from ..extract import HTMLContentExtractor
                self.html_extractor = HTMLContentExtractor(base_domain=self.base_domain)
                logger.info("HTML extractor initialized")
            except ImportError as e:
                logger.error(f"Failed to import HTMLContentExtractor: {e}")
                raise
        return self.html_extractor
    
    def crawl_category(self, category: RSSCategory, filter_by_today: bool = True) -> int:
        """
        Crawl a single category and its subcategories
        
        Args:
            category: RSSCategory to crawl
            filter_by_today: Whether to only get articles from today (Vietnam timezone)
            
        Returns:
            Number of new articles collected
        """
        category_name = category.name
        category_url = category.url
        
        filter_info = " (today only)" if filter_by_today else ""
        logger.info(f"Crawling category: {category_name}{filter_info}")
        
        try:
            # Crawl main category (parser handles date filtering now)
            articles = self.parser.parse_rss_feed(category_url, category_name, filter_by_today)
            # Filter new articles using MongoDB storage
            new_articles = []
            for article in articles:
                # Check if article already exists in MongoDB
                if not self.storage.is_article_exists(article.guid):
                    new_articles.append(article)
            
            # Save new articles to MongoDB and file in one batch
            if new_articles:
                self.storage.save_articles_to_file(new_articles, category_name)
                logger.info(f"Saved {len(new_articles)} new articles from {category_name}")
            
            # Crawl subcategories
            for subcat in category.subcategories:
                try:
                    # Use main category as the category name for subcategories
                    subcat_articles = self.parser.parse_rss_feed(subcat.url, category_name, filter_by_today)
                    
                    # Filter new articles using MongoDB storage
                    new_subcat_articles = []
                    for article in subcat_articles:
                        # Check if article already exists in MongoDB
                        if not self.storage.is_article_exists(article.guid):
                            new_subcat_articles.append(article)
                    
                    if new_subcat_articles:
                        self.storage.save_articles_to_file(new_subcat_articles, category_name)
                        logger.info(f"Saved {len(new_subcat_articles)} new articles from {subcat.name}")
                    
                    time.sleep(0.5)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Error crawling subcategory {subcat.name}: {e}")
            
            total_new = len(new_articles)
            logger.info(f"Category {category_name}: {total_new} new articles")
            
            time.sleep(1)  # Rate limiting between main categories
            return total_new
            
        except Exception as e:
            logger.error(f"Error crawling category {category_name}: {e}")
            return 0
    
    def extract_html_for_articles(self, articles: List[Article], extract_delay: Optional[float] = None) -> Dict[str, Any]:
        """
        Extract HTML content for a list of articles
        
        Args:
            articles: List of Article objects to extract HTML from
            extract_delay: Delay between extraction requests (defaults to config)
            
        Returns:
            Extraction results summary
        """
        if not articles:
            logger.info("No articles to extract HTML from")
            return {'total_articles': 0, 'successful_extractions': 0, 'failed_extractions': 0}
        
        logger.info(f"Starting HTML extraction for {len(articles)} articles")
        
        try:
            # Use HTML extractor for batch processing
            extractor = self._get_html_extractor()
            results = extractor.extract_batch(articles, delay=extract_delay)
            
            # Update articles with extraction results
            successful_count = 0
            failed_count = 0
            
            for i, article in enumerate(articles):
                if i < len(results['results']):
                    extraction_result = results['results'][i]
                    article.update_html_content(extraction_result)
                    
                    if extraction_result.get('extraction_success', False):
                        successful_count += 1
                        # Update article in MongoDB with HTML content
                        self.storage.save_article_to_db(article)
                    else:
                        failed_count += 1
            
            logger.info(f"HTML extraction completed: {successful_count}/{len(articles)} successful")
            
            return {
                'total_articles': len(articles),
                'successful_extractions': successful_count,
                'failed_extractions': failed_count,
                'extraction_time': results.get('extraction_time', 0)
            }
            
        except Exception as e:
            logger.error(f"Error during HTML extraction: {e}")
            return {
                'total_articles': len(articles),
                'successful_extractions': 0,
                'failed_extractions': len(articles),
                'error': str(e)
            }
    
    def crawl_with_html_extraction(self, filter_by_today: bool = True, extract_html: bool = True) -> CrawlSession:
        """
        Crawl all categories and optionally extract HTML content
        
        Args:
            filter_by_today: Whether to only get articles from today
            extract_html: Whether to extract HTML content for articles
            
        Returns:
            CrawlSession with extraction results
        """
        logger.info(f"Starting comprehensive crawl session{' with HTML extraction' if extract_html else ''}")
        
        # Start regular crawling
        session = self.crawl_all_categories(filter_by_today)
        session.html_extraction_enabled = extract_html
        
        if extract_html and session.total_articles > 0:
            logger.info(f"Starting HTML extraction for {session.total_articles} articles")
            
            try:
                # Get recent articles from MongoDB
                today = date.today()
                
                # Find articles from today
                start_date = datetime.combine(today, datetime.min.time())
                end_date = datetime.combine(today, datetime.max.time())
                
                recent_articles_dicts = self.storage.repository.find_articles_by_date_range(
                    start_date, end_date
                )
                
                # Convert dicts back to Article objects (simplified conversion)
                articles = []
                for article_dict in recent_articles_dicts[:50]:  # Limit to 50 for performance
                    if isinstance(article_dict, dict):
                        content = article_dict.get('content', {})
                        article = Article(
                            title=content.get('headline', ''),
                            link=article_dict.get('source', {}).get('url', ''),
                            description=content.get('summary', ''),
                            pub_date=content.get('rss_pub_date', ''),
                            guid=content.get('rss_guid', ''),
                            category=article_dict.get('rss_category', ''),
                            source=article_dict.get('source', {}).get('name', 'vietstock'),
                            crawled_at=article_dict.get('created_at', ''),
                            image=content.get('image_url'),
                            description_text=content.get('description_text', '')
                        )
                        
                        # Skip if HTML already extracted
                        if not content.get('html_extraction_success', False):
                            articles.append(article)
                
                if articles:
                    # Extract HTML content
                    extraction_results = self.extract_html_for_articles(articles)
                    
                    # Update session with extraction info
                    session.html_extraction_results = extraction_results
                    logger.info(f"HTML extraction completed: {extraction_results}")
                
            except Exception as e:
                logger.error(f"Error during HTML extraction phase: {e}")
                session.html_extraction_error = str(e)
        
        return session
    
    def crawl_all_categories(self, filter_by_today: bool = True) -> CrawlSession:
        """
        Crawl all categories from Vietstock
        
        Args:
            filter_by_today: Whether to only get articles from today (Vietnam timezone)
            
        Returns:
            CrawlSession object with results
        """
        filter_info = " (today only)" if filter_by_today else ""
        logger.info(f"Starting Vietstock RSS crawl session{filter_info}")
        
        session = CrawlSession(
            base_url=self.base_url,
            output_directory=str(self.storage.output_dir)
        )
        
        try:
            # Get all categories
            categories = self.parser.get_rss_categories(self.base_url)
            
            if not categories:
                raise Exception("No categories found")
            
            logger.info(f"Found {len(categories)} categories")
            
            # Crawl each category
            total_articles = 0
            categories_data = []
            
            for i, category in enumerate(categories, 1):
                logger.info(f"[{i}/{len(categories)}] Processing: {category.name}")
                
                new_count = self.crawl_category(category, filter_by_today)
                total_articles += new_count
                
                categories_data.append({
                    'name': category.name,
                    'url': category.url,
                    'new_articles_count': new_count,
                    'subcategories_count': len(category.subcategories)
                })
            
            # Update session
            session.categories = categories_data
            session.total_articles = total_articles
            
            # Get detailed summary from MongoDB
            summary_categories = self.storage.get_categories_summary()
            if summary_categories:
                session.categories = summary_categories
            
            # Save summary to MongoDB and file
            self.storage.save_crawl_summary(session)
            
            logger.info(f"Crawl session completed. Total new articles: {total_articles}")
            
        except Exception as e:
            logger.error(f"Crawl session failed: {e}")
            session.total_articles = 0
        
        return session
    
    def get_crawl_statistics(self) -> Dict[str, Any]:
        """Get comprehensive crawling statistics from MongoDB"""
        try:
            # Get statistics from MongoDB repository
            mongo_stats = self.storage.get_articles_statistics()
            
            # Get recent crawl sessions
            recent_sessions = self.storage.repository.get_recent_crawl_sessions(10)
            
            return {
                'storage_backend': 'mongodb',
                'database_name': self.storage.storage_config.database_name,
                'mongo_statistics': mongo_stats,
                'export_directory': self.storage.output_dir,
                'source': self.storage.source_config.name,
                'recent_crawl_sessions': [
                    {
                        'id': session.get('_id'),
                        'created_at': session.get('created_at'),
                        'total_articles_found': session.get('total_articles_found'),
                        'new_articles_saved': session.get('new_articles_saved'),
                        'success_rate': session.get('success_rate'),
                        'duration_seconds': session.get('duration_seconds'),
                        'success': session.get('success')
                    }
                    for session in recent_sessions if isinstance(session, dict)
                ],
                'last_updated': datetime.now(datetime.timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {
                'storage_backend': 'mongodb',
                'database_name': self.storage.storage_config.database_name,
                'error': str(e),
                'last_updated': datetime.now(datetime.timezone.utc).isoformat()
            }
    
    def close(self):
        """Close crawler and cleanup resources"""
        try:
            if self.storage:
                self.storage.close()
            logger.info("VietstockMongoCrawlerService closed")
        except Exception as e:
            logger.error(f"Error closing crawler service: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()