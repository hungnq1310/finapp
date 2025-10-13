"""
Vietstock Crawler Service
"""

import time
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

from .models import Article, RSSCategory, CrawlSession
from .parser import RSSParser
from .storage import StorageService

logger = logging.getLogger(__name__)


class VietstockCrawlerService:
    """Main crawler service for Vietstock RSS feeds"""
    
    def __init__(self, base_url: str = "https://vietstock.vn/rss", 
                 base_dir: str = "data", source_name: str = "vietstock",
                 db_path: str = "vietstock_crawler.db"):
        self.base_url = base_url
        self.base_domain = "https://vietstock.vn"
        
        # Initialize services
        self.parser = RSSParser(self.base_domain)
        self.storage = StorageService(base_dir, source_name, db_path)
        
        logger.info(f"✅ VietstockCrawlerService initialized - storing in {self.storage.output_dir}")
    
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
        logger.info(f"📁 Crawling category: {category_name}{filter_info}")
        
        try:
            # Crawl main category (parser handles date filtering now)
            articles = self.parser.parse_rss_feed(category_url, category_name, filter_by_today)
            new_articles = []
            
            # Filter new articles (parser already filtered by date)
            for article in articles:
                # Check if article already exists
                if not self.storage.is_article_exists(article.guid):
                    self.storage.save_article_to_db(article)
                    new_articles.append(article)
            
            # Save new articles to file
            if new_articles:
                self.storage.save_articles_to_file(new_articles, category_name)
            
            # Crawl subcategories
            for subcat in category.subcategories:
                subcat_name = f"{category_name}/{subcat.name}"
                try:
                    subcat_articles = self.parser.parse_rss_feed(subcat.url, subcat_name, filter_by_today)
                    new_subcat_articles = []
                    
                    # Filter new articles (parser already filtered by date)
                    for article in subcat_articles:
                        # Check if article already exists
                        if not self.storage.is_article_exists(article.guid):
                            self.storage.save_article_to_db(article)
                            new_subcat_articles.append(article)
                    
                    if new_subcat_articles:
                        self.storage.save_articles_to_file(new_subcat_articles, subcat_name)
                    
                    time.sleep(0.5)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"❌ Error crawling subcategory {subcat_name}: {e}")
                    self.storage.log_crawl_session(subcat_name, 0, False, str(e))
            
            total_new = len(new_articles)
            self.storage.log_crawl_session(category_name, total_new, True)
            
            time.sleep(1)  # Rate limiting between main categories
            return total_new
            
        except Exception as e:
            logger.error(f"❌ Error crawling category {category_name}: {e}")
            self.storage.log_crawl_session(category_name, 0, False, str(e))
            return 0
    
    def crawl_all_categories(self, filter_by_today: bool = True) -> CrawlSession:
        """
        Crawl all categories from Vietstock
        
        Args:
            filter_by_today: Whether to only get articles from today (Vietnam timezone)
            
        Returns:
            CrawlSession object with results
        """
        filter_info = " (today only)" if filter_by_today else ""
        logger.info(f"🚀 Starting Vietstock RSS crawl session{filter_info}")
        
        session = CrawlSession(
            base_url=self.base_url,
            output_directory=self.storage.output_dir
        )
        
        try:
            # Get all categories
            categories = self.parser.get_rss_categories(self.base_url)
            
            if not categories:
                raise Exception("No categories found")
            
            logger.info(f"📋 Found {len(categories)} categories")
            
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
            
            # Get detailed summary
            summary_categories = self.storage.get_categories_summary()
            session.categories = summary_categories
            
            # Save summary
            self.storage.save_crawl_summary(session)
            
            logger.info(f"✅ Crawl session completed. Total new articles: {total_articles}")
            
        except Exception as e:
            logger.error(f"❌ Crawl session failed: {e}")
            session.total_articles = 0
        
        return session
    
    def get_crawl_statistics(self) -> Dict[str, Any]:
        """Get crawling statistics"""
        try:
            import sqlite3
            conn = sqlite3.connect(self.storage.db_path)
            cursor = conn.cursor()
            
            # Get total articles from DB
            cursor.execute("SELECT COUNT(*) FROM articles")
            total_articles_db = cursor.fetchone()[0]
            
            # Get recent crawl logs
            cursor.execute("""
                SELECT category, articles_count, crawl_time, success, error_message 
                FROM crawl_log 
                ORDER BY crawl_time DESC 
                LIMIT 10
            """)
            recent_logs = cursor.fetchall()
            
            conn.close()
            
            # Get articles statistics from unified file
            articles_stats = self.storage.get_articles_statistics()
            
            return {
                'total_articles_db': total_articles_db,
                'total_articles_file': articles_stats.get('total_articles', 0),
                'output_directory': self.storage.output_dir,
                'source': self.storage.source_name,
                'recent_logs': [
                    {
                        'category': log[0],
                        'articles_count': log[1],
                        'crawl_time': log[2],
                        'success': log[3],
                        'error_message': log[4]
                    }
                    for log in recent_logs
                ],
                'categories_summary': self.storage.get_categories_summary(),
                'articles_statistics': articles_stats
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting statistics: {e}")
            return {
                'total_articles_db': 0,
                'total_articles_file': 0,
                'output_directory': self.storage.output_dir,
                'source': self.storage.source_name,
                'recent_logs': [],
                'categories_summary': [],
                'articles_statistics': {},
                'error': str(e)
            }