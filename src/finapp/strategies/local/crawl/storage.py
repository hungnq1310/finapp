"""
MongoDB Storage service for Vietstock crawler

This service replaces the SQLite-based storage with MongoDB repository
while maintaining compatibility with the existing crawler logic.
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
import uuid

from .models import Article, CrawlSession, RSSCategory
from finapp.database.vietstock import VietstockRepository
from finapp.schema.vietstock import VietstockArticle, VietstockSource, VietstockContent, VietstockCrawlSession

logger = logging.getLogger(__name__)


class StorageService:
    """Service for managing data storage using MongoDB"""
    
    def __init__(self, base_dir: str = "data", source_name: str = "vietstock", 
                 mongo_uri: str = None, database_name: str = "financial_news"):
        self.base_dir = base_dir
        self.source_name = source_name
        self.mongo_uri = mongo_uri or os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        self.database_name = database_name or os.getenv("DATABASE_NAME", "financial_news")
        
        # Initialize MongoDB repository
        self.repository = VietstockRepository(self.mongo_uri, self.database_name)
        
        # Set output directory for JSON exports (keeping for compatibility)
        self.output_dir = os.path.join(base_dir, source_name)
        os.makedirs(self.output_dir, exist_ok=True)
        
        logger.info(f"✅ MongoDB Storage service initialized with database: {self.database_name}")
    
    def is_article_exists(self, guid: str) -> bool:
        """Check if article already exists in MongoDB"""
        try:
            article = self.repository.find_article_by_guid(guid)
            return article is not None
        except Exception as e:
            logger.error(f"❌ Error checking article existence: {e}")
            return False
    
    def save_article_to_db(self, article: Article) -> bool:
        """Save article to MongoDB using Vietstock schema"""
        try:
            # Check if article already exists in MongoDB
            existing_article = self.repository.find_article_by_guid(article.guid)
            
            if existing_article:
                # Update existing article with HTML content only
                success = self._update_article_html_content(existing_article, article)
                if success:
                    logger.debug(f"✅ Updated HTML content for existing article: {article.guid}")
                return success
            else:
                # Convert Article model to VietstockArticle (new article)
                vietstock_article = self._convert_to_vietstock_article(article)
                
                # Save to MongoDB
                success = self.repository.save_article(vietstock_article)
                
                if success:
                    logger.debug(f"✅ Created new article in MongoDB: {article.guid}")
                
                return success
            
        except Exception as e:
            logger.error(f"❌ Error saving article to MongoDB: {e}")
            return False
    
    def _update_article_html_content(self, existing_doc: Dict, new_article: Article) -> bool:
        """Update only HTML content fields for an existing article"""
        try:
            # Preserve the existing _id
            existing_id = existing_doc.get('_id')
            
            # Update HTML content fields in the existing document
            updates = {
                'content.html_extracted_at': datetime.fromisoformat(str(new_article.html_extracted_at)) if new_article.html_extracted_at else None,
                'content.html_extraction_success': new_article.html_extraction_success,
                'content.raw_html': new_article.raw_html,
                'content.main_content': new_article.main_content,
                'content.content_hash': new_article.content_hash,
                'last_updated': datetime.now()
            }
            
            # Update in MongoDB using the existing _id
            collection = self.repository.db.vietstock_articles
            result = collection.update_one(
                {'_id': existing_id},
                {'$set': updates}
            )
            
            # Also update JSON file with HTML content
            if result.modified_count > 0:
                self._update_html_in_json_file(new_article)
            
            return result.modified_count > 0
            
        except Exception as e:
            logger.error(f"❌ Error updating HTML content for article {new_article.guid}: {e}")
            return False
    
    def _update_html_in_json_file(self, article: Article) -> bool:
        """Update HTML content for an article in the JSON file"""
        try:
            # Get current articles file
            current_file = self.get_current_articles_file()
            
            if not os.path.exists(current_file):
                logger.warning(f"⚠️ JSON file {current_file} does not exist, cannot update HTML content")
                return False
            
            # Load existing data
            with open(current_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Find and update the article by GUID
            article_found = False
            for json_article in data.get('articles', []):
                if json_article.get('guid') == article.guid:
                    # Update HTML fields
                    json_article['raw_html'] = article.raw_html
                    json_article['main_content'] = article.main_content
                    json_article['content_hash'] = article.content_hash
                    json_article['html_extracted_at'] = article.html_extracted_at.isoformat() if article.html_extracted_at and hasattr(article.html_extracted_at, 'isoformat') else article.html_extracted_at
                    json_article['html_extraction_success'] = article.html_extraction_success
                    article_found = True
                    break
            
            if article_found:
                # Update the file
                data['last_updated'] = datetime.now().isoformat()
                with open(current_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                # Also update latest.json
                latest_file = os.path.join(self.output_dir, "latest.json")
                with open(latest_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                logger.debug(f"✅ Updated HTML content in JSON file for article: {article.guid}")
                return True
            else:
                logger.warning(f"⚠️ Article {article.guid} not found in JSON file for HTML update")
                return False
            
        except Exception as e:
            logger.error(f"❌ Error updating HTML content in JSON file for article {article.guid}: {e}")
            return False
    
    def save_articles_batch(self, articles: List[Article]) -> Dict[str, int]:
        """Save multiple articles to MongoDB in batch"""
        if not articles:
            return {"success": 0, "failed": 0, "duplicates": 0}
        
        try:
            # Convert all articles to VietstockArticle format
            vietstock_articles = []
            for article in articles:
                try:
                    vietstock_article = self._convert_to_vietstock_article(article)
                    vietstock_articles.append(vietstock_article)
                except Exception as e:
                    logger.warning(f"⚠️ Failed to convert article {article.guid}: {e}")
            
            # Save batch to MongoDB
            results = self.repository.save_articles_batch(vietstock_articles)
            
            logger.info(f"📊 Batch save to MongoDB: {results}")
            return results
            
        except Exception as e:
            logger.error(f"❌ Error in batch save to MongoDB: {e}")
            return {"success": 0, "failed": len(articles), "duplicates": 0}
    
    def save_articles_to_file(self, articles: List[Article], category_name: str = "") -> bool:
        """
        Save articles to JSON file (keeping for compatibility and export purposes)
        This method now serves as an export/backup function
        """
        if not articles:
            return False
        
        try:
            # Save to MongoDB first
            batch_results = self.save_articles_batch(articles)
            
            # Ensure JSON file exists (restore from MongoDB if missing)
            if not self.ensure_json_file_exists():
                logger.warning("⚠️ Could not ensure JSON file exists, proceeding with new articles only")
            
            # Export to JSON file (keeping existing structure for compatibility)
            current_file = self.get_current_articles_file()
            
            # Load existing data if file exists
            existing_data = {
                'source': self.source_name,
                'created_at': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat(),
                'total_articles': 0,
                'articles': []
            }
            
            if os.path.exists(current_file):
                try:
                    with open(current_file, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                except Exception as e:
                    logger.warning(f"⚠️ Could not load existing file {current_file}: {e}")
            
            # Add new articles data
            new_articles_data = [article.to_dict() for article in articles]
            existing_articles = existing_data.get('articles', [])
            
            # Combine existing and new articles
            all_articles = existing_articles + new_articles_data
            
            # Remove duplicates by GUID
            seen_guids = set()
            unique_articles = []
            for article in all_articles:
                guid = article.get('guid')
                if guid and guid not in seen_guids:
                    seen_guids.add(guid)
                    unique_articles.append(article)
            
            # Sort by crawled_at (newest first)
            unique_articles.sort(key=lambda x: x.get('crawled_at', ''), reverse=True)
            
            # Update data
            data = {
                'source': self.source_name,
                'created_at': existing_data.get('created_at', datetime.now().isoformat()),
                'last_updated': datetime.now().isoformat(),
                'total_articles': len(unique_articles),
                'mongo_sync_stats': batch_results,
                'articles': unique_articles
            }
            
            # Save to daily file
            with open(current_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # Also save as latest.json (in root output dir)
            latest_file = os.path.join(self.output_dir, "latest.json")
            with open(latest_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"💾 Exported {len(new_articles_data)} articles to {current_file}")
            logger.info(f"📊 MongoDB sync stats: {batch_results}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving articles to file: {e}")
            return False
    
    def get_current_articles_file(self) -> str:
        """Get current daily articles file path"""
        date_str = datetime.now().strftime("%Y%m%d")
        daily_dir = os.path.join(self.output_dir, date_str)
        os.makedirs(daily_dir, exist_ok=True)
        
        articles_file = os.path.join(daily_dir, f"articles_{date_str}.json")
        return articles_file
    
    def restore_from_mongodb(self, date_filter: Optional[str] = None) -> bool:
        """
        Restore JSON files from MongoDB when they are missing
        
        Args:
            date_filter: Specific date in YYYYMMDD format (default: today)
            
        Returns:
            True if restoration was successful
        """
        try:
            # Determine target date
            if date_filter:
                target_date = datetime.strptime(date_filter, "%Y%m%d").date()
            else:
                target_date = datetime.now().date()
            
            # Get date range for the target date
            start_date = datetime.combine(target_date, datetime.min.time())
            end_date = datetime.combine(target_date, datetime.max.time())
            
            # Fetch articles from MongoDB for the specific date
            articles_dicts = self.repository.find_articles_by_date_range(start_date, end_date)
            
            if not articles_dicts:
                logger.info(f"ℹ️ No articles found in MongoDB for {target_date}")
                return False
            
            logger.info(f"🔄 Found {len(articles_dicts)} articles in MongoDB for {target_date}")
            
            # Convert MongoDB documents to Article format
            articles = []
            for article_dict in articles_dicts:
                try:
                    if isinstance(article_dict, dict):
                        content = article_dict.get('content', {})
                        article = {
                            "title": content.get('headline', ''),
                            "link": article_dict.get('source', {}).get('url', ''),
                            "description": content.get('summary', ''),
                            "pub_date": content.get('rss_pub_date', ''),
                            "guid": content.get('rss_guid', ''),
                            "category": article_dict.get('rss_category', ''),
                            "source": article_dict.get('source', {}).get('name', 'vietstock'),
                            "crawled_at": article_dict.get('created_at', '').isoformat() if article_dict.get('created_at') else '',
                            "image": content.get('image_url'),
                            "description_text": content.get('description_text', ''),
                            # HTML content fields
                            "raw_html": content.get('raw_html'),
                            "main_content": content.get('main_content'),
                            "content_hash": content.get('content_hash'),
                            "html_extracted_at": content.get('html_extracted_at').isoformat() if content.get('html_extracted_at') else None,
                            "html_extraction_success": content.get('html_extraction_success', False)
                        }
                        articles.append(article)
                except Exception as e:
                    logger.warning(f"⚠️ Failed to convert article from MongoDB: {e}")
            
            if not articles:
                logger.warning("⚠️ No valid articles could be converted from MongoDB")
                return False
            
            # Save to JSON file using existing method
            date_str = target_date.strftime("%Y%m%d")
            daily_dir = os.path.join(self.output_dir, date_str)
            os.makedirs(daily_dir, exist_ok=True)
            
            # Create JSON structure
            data = {
                "source": self.source_name,
                "created_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                "total_articles": len(articles),
                "mongo_sync_stats": {
                    "success": len(articles),
                    "failed": 0,
                    "duplicates": 0,
                    "restored_from_mongodb": True
                },
                "articles": articles
            }
            
            # Save to daily file
            articles_file = os.path.join(daily_dir, f"articles_{date_str}.json")
            with open(articles_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # Also save as latest.json
            latest_file = os.path.join(self.output_dir, "latest.json")
            with open(latest_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ Restored {len(articles)} articles from MongoDB to {articles_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error restoring from MongoDB: {e}")
            return False
    
    def ensure_json_file_exists(self, date_filter: Optional[str] = None) -> bool:
        """
        Ensure JSON file exists, restore from MongoDB if missing
        
        Args:
            date_filter: Specific date in YYYYMMDD format (default: today)
            
        Returns:
            True if file exists or was successfully restored
        """
        try:
            # Determine target date and file path
            if date_filter:
                target_date = datetime.strptime(date_filter, "%Y%m%d").date()
            else:
                target_date = datetime.now().date()
            
            date_str = target_date.strftime("%Y%m%d")
            daily_dir = os.path.join(self.output_dir, date_str)
            articles_file = os.path.join(daily_dir, f"articles_{date_str}.json")
            
            # Check if file already exists
            if os.path.exists(articles_file):
                logger.debug(f"ℹ️ JSON file already exists: {articles_file}")
                return True
            
            logger.warning(f"⚠️ JSON file missing: {articles_file}")
            
            # Try to restore from MongoDB
            restored = self.restore_from_mongodb(date_filter)
            if restored:
                logger.info(f"✅ Successfully restored JSON file from MongoDB")
                return True
            else:
                logger.warning(f"⚠️ Could not restore JSON file from MongoDB")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error ensuring JSON file exists: {e}")
            return False
    
    def save_crawl_summary(self, session: CrawlSession):
        """Save crawl session summary to MongoDB and file"""
        try:
            # Convert to VietstockCrawlSession and save to MongoDB
            vietstock_session = self._convert_to_crawl_session(session)
            self.repository.save_crawl_session(vietstock_session)
            
            # Also save to file for compatibility
            daily_dir = self.get_daily_folder_path()
            date_str = daily_dir.split('/')[-1]
            summary_file = os.path.join(daily_dir, f"summary_{date_str}.json")
            
            # Add MongoDB stats to session data
            session_data = session.to_dict()
            session_data['mongo_database'] = self.database_name
            session_data['mongo_sync'] = True
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(session_data, f, ensure_ascii=False, indent=2)
            
            # Also save as latest summary
            latest_file = os.path.join(self.output_dir, "summary.json")
            with open(latest_file, 'w', encoding='utf-8') as f:
                json.dump(session_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"📊 Crawl summary saved to MongoDB and {summary_file}")
            
        except Exception as e:
            logger.error(f"❌ Error saving summary: {e}")
    
    def get_daily_folder_path(self) -> str:
        """Get daily folder path for storing files"""
        date_str = datetime.now().strftime("%Y%m%d")
        daily_dir = os.path.join(self.output_dir, date_str)
        os.makedirs(daily_dir, exist_ok=True)
        return daily_dir
    
    def get_categories_summary(self) -> List[Dict[str, Any]]:
        """Get categories summary from MongoDB"""
        try:
            stats = self.repository.get_articles_statistics()
            return stats.get('categories', [])
        except Exception as e:
            logger.error(f"❌ Error getting categories summary: {e}")
            return []
    
    def get_articles_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics from MongoDB"""
        try:
            # Get MongoDB statistics
            mongo_stats = self.repository.get_articles_statistics()
            
            # Add file-based statistics for compatibility
            current_file = self.get_current_articles_file()
            file_stats = {
                'file_path': current_file,
                'file_exists': os.path.exists(current_file),
                'file_size_mb': os.path.getsize(current_file) / (1024*1024) if os.path.exists(current_file) else 0
            }
            
            # Combine statistics
            combined_stats = {
                **mongo_stats,
                'file_stats': file_stats,
                'storage_backend': 'mongodb',
                'database_name': self.database_name,
                'export_directory': self.output_dir
            }
            
            return combined_stats
            
        except Exception as e:
            logger.error(f"❌ Error getting articles statistics: {e}")
            return {
                'error': str(e),
                'storage_backend': 'mongodb',
                'database_name': self.database_name
            }
    
    def reset_database(self):
        """Reset MongoDB collections for testing purposes"""
        try:
            # This would require implementing a reset method in the repository
            # For now, just log the action
            logger.warning("⚠️ MongoDB reset not implemented yet - use database management tools")
            logger.info("🗑️ To reset MongoDB, manually drop the collections or database")
            
        except Exception as e:
            logger.error(f"❌ Error resetting database: {e}")
    
    def _convert_to_vietstock_article(self, article: Article) -> VietstockArticle:
        """Convert Article model to VietstockArticle schema"""
        try:
            # Create VietstockSource
            source = VietstockSource(
                url=article.link,
                rss_url=None,  # Could be extracted from category context
                category=article.category
            )
            
            # Parse publication date
            pub_date = datetime.now()
            if article.pub_date:
                try:
                    # Try to parse RSS date format
                    from email.utils import parsedate_to_datetime
                    pub_date = parsedate_to_datetime(article.pub_date)
                except:
                    # Fallback to current time
                    pub_date = datetime.now()
            
            # Create VietstockContent
            content = VietstockContent(
                headline=article.title,
                summary=article.description_text or article.description,
                body=article.main_content or article.description,
                rss_description=article.description,
                rss_guid=article.guid,
                rss_pub_date=article.pub_date,
                image_url=article.image,
                description_text=article.description_text,
                raw_html=article.raw_html,
                main_content=article.main_content,
                content_hash=article.content_hash,
                html_extracted_at=datetime.fromisoformat(str(article.html_extracted_at)) if article.html_extracted_at else None,
                html_extraction_success=article.html_extraction_success
            )
            
            # Generate unique ID
            article_id = str(uuid.uuid4())
            
            # Create VietstockArticle
            vietstock_article = VietstockArticle(
                id=article_id,
                source=source,
                content=content,
                published_at=pub_date,
                rss_category=article.category,
                crawled_at=datetime.fromisoformat(str(article.crawled_at)) if article.crawled_at else datetime.now()
            )
            
            return vietstock_article
            
        except Exception as e:
            logger.error(f"❌ Error converting article to Vietstock schema: {e}")
            raise
    
    def _convert_to_crawl_session(self, session: CrawlSession) -> VietstockCrawlSession:
        """Convert CrawlSession to VietstockCrawlSession schema"""
        try:
            # Generate unique ID
            session_id = str(uuid.uuid4())
            
            # Extract categories from session data
            categories = []
            if hasattr(session, 'categories') and session.categories:
                categories = [cat.get('name', '') for cat in session.categories if isinstance(cat, dict)]
            
            # Create VietstockCrawlSession
            vietstock_session = VietstockCrawlSession(
                id=session_id,
                source_base_url=getattr(session, 'base_url', ''),
                categories_crawled=categories,
                total_articles_found=getattr(session, 'total_articles', 0),
                new_articles_saved=getattr(session, 'total_articles', 0),
                html_extraction_enabled=getattr(session, 'html_extraction_enabled', False),
                html_extraction_stats=getattr(session, 'html_extraction_results', {}),
                success=True,
                error_message=None,
                created_at=datetime.fromisoformat(session.crawled_at) if session.crawled_at else datetime.now()
            )
            
            return vietstock_session
            
        except Exception as e:
            logger.error(f"❌ Error converting crawl session to Vietstock schema: {e}")
            raise
    
    def close(self):
        """Close MongoDB connection"""
        if self.repository:
            self.repository.close()
            logger.info("🔌 MongoDB storage service closed")