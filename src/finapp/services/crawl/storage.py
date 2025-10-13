"""
Storage service for Vietstock crawler
"""

import os
import json
import sqlite3
from datetime import datetime
from typing import List, Optional, Dict, Any
import logging

from .models import Article, CrawlSession, RSSCategory

logger = logging.getLogger(__name__)


class StorageService:
    """Service for managing data storage (JSON files and SQLite tracking)"""
    
    def __init__(self, base_dir: str = "data", source_name: str = "vietstock", db_path: str = "vietstock_crawler.db"):
        self.base_dir = base_dir
        self.source_name = source_name
        self.output_dir = os.path.join(base_dir, source_name)
        self.db_path = db_path
        self._init_database()
        self._create_output_structure()
    
    def _init_database(self):
        """Initialize SQLite database for tracking crawled articles"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guid TEXT UNIQUE NOT NULL,
                    title TEXT,
                    link TEXT,
                    pub_date TEXT,
                    category TEXT,
                    crawled_at TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS crawl_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category TEXT,
                    articles_count INTEGER,
                    crawl_time TEXT,
                    success BOOLEAN,
                    error_message TEXT
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("✅ Database initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            raise
    
    def _create_output_structure(self):
        """Create output directory structure"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"✅ Created output directory: {self.output_dir}")
    
    def is_article_exists(self, guid: str) -> bool:
        """Check if article already exists in database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM articles WHERE guid = ?", (guid,))
            result = cursor.fetchone()
            conn.close()
            return result is not None
        except Exception as e:
            logger.error(f"❌ Error checking article existence: {e}")
            return False
    
    def save_article_to_db(self, article: Article) -> bool:
        """Save article to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO articles (guid, title, link, pub_date, category, crawled_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                article.guid,
                article.title,
                article.link,
                article.pub_date,
                article.category,
                article.crawled_at
            ))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"❌ Error saving article to DB: {e}")
            return False
    
    def save_articles_to_file(self, articles: List[Article], category_name: str = "") -> bool:
        """Save articles to unified JSON file - only append new articles"""
        if not articles:
            return False
        
        try:
            # Get current unified file or create new one
            current_file = self.get_current_articles_file()
            
            # Simply append new articles - don't load existing data for performance
            # Database already handles duplicates, so we trust the input
            new_articles_data = [article.to_dict() for article in articles]
            
            logger.info(f"💾 Appending {len(new_articles_data)} new articles to file")
            
            # For now, use simple append strategy - create file with only new articles
            # In a production system, you might want a more sophisticated append-only strategy
            data = {
                'source': self.source_name,
                'created_at': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat(),
                'total_articles': len(new_articles_data),
                'articles': new_articles_data
            }
            
            with open(current_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # Also save as latest.json
            latest_file = os.path.join(self.output_dir, "latest.json")
            with open(latest_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"💾 Saved {len(articles)} new articles to unified file")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving articles to file: {e}")
            return False
    
    def get_current_articles_file(self) -> str:
        """Get current unified articles file path"""
        # Check if there's an existing file for today
        date_str = datetime.now().strftime("%Y%m%d")
        today_file = os.path.join(self.output_dir, f"articles_{date_str}.json")
        
        if os.path.exists(today_file):
            return today_file
        
        # Look for the most recent file
        if os.path.exists(self.output_dir):
            files = [f for f in os.listdir(self.output_dir) if f.startswith("articles_") and f.endswith(".json")]
            if files:
                files.sort(reverse=True)
                recent_file = os.path.join(self.output_dir, files[0])
                return recent_file
        
        # Create new file for today
        return today_file
    
    def archive_current_file(self):
        """Archive current file with timestamp and create new one"""
        try:
            current_file = self.get_current_articles_file()
            if os.path.exists(current_file):
                # Create archive filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                archive_file = os.path.join(self.output_dir, f"archive_articles_{timestamp}.json")
                
                # Move current file to archive
                import shutil
                shutil.move(current_file, archive_file)
                
                logger.info(f"📦 Archived current file to: {archive_file}")
                
        except Exception as e:
            logger.error(f"❌ Error archiving file: {e}")
    
    def log_crawl_session(self, category: str, count: int, success: bool, error_msg: Optional[str] = None):
        """Log crawl session to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO crawl_log (category, articles_count, crawl_time, success, error_message)
                VALUES (?, ?, ?, ?, ?)
            ''', (category, count, datetime.now().isoformat(), success, error_msg))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"❌ Error logging crawl session: {e}")
    
    def save_crawl_summary(self, session: CrawlSession):
        """Save crawl session summary"""
        try:
            summary_file = os.path.join(self.output_dir, "summary.json")
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(session.to_dict(), f, ensure_ascii=False, indent=2)
            
            logger.info(f"📊 Summary saved to: {summary_file}")
        except Exception as e:
            logger.error(f"❌ Error saving summary: {e}")
    
    def get_categories_summary(self) -> List[Dict[str, Any]]:
        """Get summary from unified articles file"""
        categories = []
        
        try:
            current_file = self.get_current_articles_file()
            if os.path.exists(current_file):
                with open(current_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    articles = data.get('articles', [])
                
                # Count articles by category
                category_counts = {}
                latest_crawl_by_category = {}
                
                for article in articles:
                    category = article.get('category', 'Unknown')
                    category_counts[category] = category_counts.get(category, 0) + 1
                    
                    # Track latest crawl time per category
                    crawl_time = article.get('crawled_at', '')
                    if crawl_time and crawl_time > latest_crawl_by_category.get(category, ''):
                        latest_crawl_by_category[category] = crawl_time
                
                # Convert to list format
                for category, count in category_counts.items():
                    categories.append({
                        'name': category,
                        'article_count': count,
                        'latest_crawl': latest_crawl_by_category.get(category)
                    })
        
        except Exception as e:
            logger.error(f"❌ Error getting categories summary: {e}")
        
        return categories
    
    def reset_database(self):
        """Reset database for testing purposes"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Clear all tables
            cursor.execute("DELETE FROM articles")
            cursor.execute("DELETE FROM crawl_log")
            
            conn.commit()
            conn.close()
            logger.info("🗑️ Database reset successfully")
            
        except Exception as e:
            logger.error(f"❌ Error resetting database: {e}")
    
    def get_articles_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics from current articles file"""
        try:
            current_file = self.get_current_articles_file()
            if not os.path.exists(current_file):
                return {
                    'total_articles': 0,
                    'source': self.source_name,
                    'categories': [],
                    'latest_crawl': None,
                    'file_info': {'file_path': current_file, 'exists': False}
                }
            
            with open(current_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                articles = data.get('articles', [])
            
            # Count by category
            category_stats = {}
            for article in articles:
                category = article.get('category', 'Unknown')
                if category not in category_stats:
                    category_stats[category] = {
                        'count': 0,
                        'latest_crawl': None,
                        'latest_title': None
                    }
                
                category_stats[category]['count'] += 1
                
                crawl_time = article.get('crawled_at', '')
                if crawl_time and (category_stats[category]['latest_crawl'] is None or crawl_time > category_stats[category]['latest_crawl']):
                    category_stats[category]['latest_crawl'] = crawl_time
                    category_stats[category]['latest_title'] = article.get('title', '')
            
            return {
                'total_articles': len(articles),
                'source': data.get('source', self.source_name),
                'created_at': data.get('created_at'),
                'last_updated': data.get('last_updated'),
                'categories': [
                    {
                        'name': cat,
                        'article_count': stats['count'],
                        'latest_crawl': stats['latest_crawl'],
                        'latest_title': stats['latest_title']
                    }
                    for cat, stats in category_stats.items()
                ],
                'file_info': {
                    'file_path': current_file,
                    'exists': True,
                    'size_mb': os.path.getsize(current_file) / (1024*1024)
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting articles statistics: {e}")
            return {
                'total_articles': 0,
                'source': self.source_name,
                'error': str(e)
            }