"""
Vietstock Crawler Service
"""

import time
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional
from typing import List, Dict, Any

from .models import Article, RSSCategory, CrawlSession
from .parser import RSSParser
from .storage import StorageService

logger = logging.getLogger(__name__)


class VietstockCrawlerService:
    """Main crawler service for Vietstock RSS feeds"""
    
    def __init__(self, base_url: str = None, base_dir: str = "data", 
                 source_name: str = "vietstock", db_path: str = None):
        # Use config values if parameters not provided
        try:
            from ...config import Config
            self.base_url = base_url or Config.CRAWLER_BASE_URL
            self.base_domain = Config.CRAWLER_BASE_DOMAIN
            db_path = db_path or Config.CRAWLER_DB_PATH
            self.html_extraction_delay = Config.CRAWLER_HTML_EXTRACTION_DELAY
            self.html_batch_size = Config.CRAWLER_HTML_BATCH_SIZE
        except ImportError:
            # Fallback to default values if config not available
            self.base_url = base_url or "https://vietstock.vn/rss"
            self.base_domain = "https://vietstock.vn"
            db_path = db_path or "data/vietstock_crawler.db"
            self.html_extraction_delay = 2.0
            self.html_batch_size = 10
        
        # Initialize services
        self.parser = RSSParser(self.base_domain)
        
        # If db_path is already absolute or contains base_dir, don't add base_dir again
        if db_path and (os.path.isabs(db_path) or db_path.startswith(base_dir)):
            self.storage = StorageService(base_dir, source_name, db_path)
        else:
            self.storage = StorageService(base_dir, source_name, os.path.join(base_dir, db_path))
        
        # HTML extractor will be initialized on demand
        self.html_extractor = None
        
        logger.info(f"✅ VietstockCrawlerService initialized - storing in {self.storage.output_dir}")
        logger.info(f"🔗 Base RSS URL: {self.base_url}")
        logger.info("🌐 HTML content extractor ready (lazy initialization)")
    
    def _get_html_extractor(self):
        """Get HTML extractor instance (lazy initialization)"""
        if self.html_extractor is None:
            try:
                from ..extract import HTMLContentExtractor
                self.html_extractor = HTMLContentExtractor(base_domain=self.base_domain)
                logger.info("🌐 HTML extractor initialized")
            except ImportError as e:
                logger.error(f"❌ Failed to import HTMLContentExtractor: {e}")
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
                try:
                    # Use main category as the category name for subcategories
                    subcat_articles = self.parser.parse_rss_feed(subcat.url, category_name, filter_by_today)
                    new_subcat_articles = []
                    
                    # Filter new articles (parser already filtered by date)
                    for article in subcat_articles:
                        # Check if article already exists
                        if not self.storage.is_article_exists(article.guid):
                            self.storage.save_article_to_db(article)
                            new_subcat_articles.append(article)
                    
                    if new_subcat_articles:
                        self.storage.save_articles_to_file(new_subcat_articles, category_name)
                    
                    time.sleep(0.5)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"❌ Error crawling subcategory {subcat.name}: {e}")
                    self.storage.log_crawl_session(category_name, 0, False, str(e))
            
            total_new = len(new_articles)
            self.storage.log_crawl_session(category_name, total_new, True)
            
            time.sleep(1)  # Rate limiting between main categories
            return total_new
            
        except Exception as e:
            logger.error(f"❌ Error crawling category {category_name}: {e}")
            self.storage.log_crawl_session(category_name, 0, False, str(e))
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
            logger.info("📄 No articles to extract HTML from")
            return {'total_articles': 0, 'successful_extractions': 0, 'failed_extractions': 0}
        
        logger.info(f"🌐 Starting HTML extraction for {len(articles)} articles")
        
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
                    else:
                        failed_count += 1
            
            logger.info(f"📊 HTML extraction completed: {successful_count}/{len(articles)} successful")
            
            return {
                'total_articles': len(articles),
                'successful_extractions': successful_count,
                'failed_extractions': failed_count,
                'extraction_time': results.get('extraction_time', 0)
            }
            
        except Exception as e:
            logger.error(f"❌ Error during HTML extraction: {e}")
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
        logger.info(f"🚀 Starting comprehensive crawl session{' with HTML extraction' if extract_html else ''}")
        
        # Start regular crawling
        session = self.crawl_all_categories(filter_by_today)
        session.html_extraction_enabled = extract_html
        
        if extract_html and session.total_articles > 0:
            logger.info(f"🌐 Starting HTML extraction for {session.total_articles} articles")
            
            try:
                # Get all articles from current daily file
                current_file = self.storage.get_current_articles_file()
                if os.path.exists(current_file):
                    import json
                    with open(current_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Convert dict back to Article objects
                    articles = []
                    for article_dict in data.get('articles', []):
                        article = Article(
                            title=article_dict.get('title', ''),
                            link=article_dict.get('link', ''),
                            description=article_dict.get('description', ''),
                            pub_date=article_dict.get('pub_date', ''),
                            guid=article_dict.get('guid', ''),
                            category=article_dict.get('category', ''),
                            source=article_dict.get('source', 'vietstock'),
                            crawled_at=article_dict.get('crawled_at', ''),
                            image=article_dict.get('image'),
                            description_text=article_dict.get('description_text', '')
                        )
                        articles.append(article)
                    
                    if articles:
                        # Extract HTML content
                        extraction_results = self.extract_html_for_articles(articles)
                        
                        # Save updated articles with HTML content
                        if extraction_results['successful_extractions'] > 0:
                            self.storage.save_articles_to_file(articles, "html_extraction")
                            logger.info(f"💾 Saved {len(articles)} articles with HTML content")
                            
                            # Update session with extraction info
                            session.html_extraction_results = extraction_results
                
            except Exception as e:
                logger.error(f"❌ Error during HTML extraction phase: {e}")
                session.html_extraction_error = str(e)
        
        return session
    
    def extract_html_for_existing_articles(self, date_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract HTML content for existing articles from a specific date
        
        Args:
            date_filter: Date in YYYYMMDD format (default: today)
            
        Returns:
            Extraction results summary
        """
        try:
            # Determine date
            if date_filter:
                target_date = date_filter
            else:
                target_date = datetime.now().strftime("%Y%m%d")
            
            logger.info(f"🌐 Starting HTML extraction for existing articles from {target_date}")
            
            # Get articles file for the date
            daily_dir = os.path.join(self.storage.output_dir, target_date)
            articles_file = os.path.join(daily_dir, f"articles_{target_date}.json")
            
            if not os.path.exists(articles_file):
                logger.warning(f"⚠️ Articles file not found: {articles_file}")
                return {
                    'success': False,
                    'message': f'Articles file not found for date {target_date}',
                    'file_path': articles_file
                }
            
            # Load existing articles
            import json
            with open(articles_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            articles_dict = data.get('articles', [])
            if not articles_dict:
                logger.info(f"📄 No articles found for date {target_date}")
                return {
                    'success': True,
                    'message': f'No articles found for date {target_date}',
                    'total_articles': 0
                }
            
            # Convert to Article objects
            articles = []
            for article_dict in articles_dict:
                # Skip if HTML already extracted
                if article_dict.get('html_extraction_success', False):
                    logger.debug(f"⏭️ Skipping already extracted: {article_dict.get('title', 'Unknown')}")
                    continue
                
                article = Article(
                    title=article_dict.get('title', ''),
                    link=article_dict.get('link', ''),
                    description=article_dict.get('description', ''),
                    pub_date=article_dict.get('pub_date', ''),
                    guid=article_dict.get('guid', ''),
                    category=article_dict.get('category', ''),
                    source=article_dict.get('source', 'vietstock'),
                    crawled_at=article_dict.get('crawled_at', ''),
                    image=article_dict.get('image'),
                    description_text=article_dict.get('description_text', '')
                )
                articles.append(article)
            
            if not articles:
                logger.info(f"📄 All articles already have HTML content for date {target_date}")
                return {
                    'success': True,
                    'message': f'All articles already have HTML content for date {target_date}',
                    'total_articles': len(articles_dict),
                    'already_extracted': len(articles_dict)
                }
            
            logger.info(f"📊 Found {len(articles)} articles without HTML content")
            
            # Extract HTML content
            extraction_results = self.extract_html_for_articles(articles)
            
            # Update the original articles dict with HTML content
            updated_articles = []
            for original_article in articles_dict:
                # Find matching article in extraction results
                for article in articles:
                    if article.guid == original_article.get('guid'):
                        # Update with HTML content
                        updated_dict = original_article.copy()
                        updated_dict.update(article.to_dict())
                        updated_articles.append(updated_dict)
                        break
                else:
                    # Keep original if no match
                    updated_articles.append(original_article)
            
            # Save updated articles back to file
            updated_data = {
                'source': data.get('source', 'vietstock'),
                'created_at': data.get('created_at', datetime.now().isoformat()),
                'last_updated': datetime.now().isoformat(),
                'total_articles': len(updated_articles),
                'articles': updated_articles
            }
            
            with open(articles_file, 'w', encoding='utf-8') as f:
                json.dump(updated_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"💾 Updated {len(updated_articles)} articles with HTML content")
            
            return {
                'success': True,
                'message': f'HTML extraction completed for date {target_date}',
                'date': target_date,
                'total_articles': len(articles_dict),
                'processed_articles': len(articles),
                'successful_extractions': extraction_results['successful_extractions'],
                'failed_extractions': extraction_results['failed_extractions'],
                'extraction_time': extraction_results.get('extraction_time', 0),
                'file_path': articles_file
            }
            
        except Exception as e:
            logger.error(f"❌ Error extracting HTML for existing articles: {e}")
            return {
                'success': False,
                'message': f'Error extracting HTML: {str(e)}',
                'date': date_filter or 'today'
            }
    
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
            
            # Get detailed summary
            summary_categories = self.storage.get_categories_summary()
            session.categories = summary_categories
            
            # Save summary
            self.storage.save_crawl_summary(session)
            
            logger.info(f"Crawl session completed. Total new articles: {total_articles}")
            
        except Exception as e:
            logger.error(f"Crawl session failed: {e}")
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
            logger.error(f" Error getting statistics: {e}")
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