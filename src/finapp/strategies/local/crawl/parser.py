"""
RSS Parser service for Vietstock crawler
"""

import feedparser
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import List, Dict, Any
import logging
from datetime import datetime, timezone, timedelta

from .models import Article, RSSCategory

logger = logging.getLogger(__name__)


class RSSParser:
    """Service for parsing RSS feeds and extracting categories"""
    
    def __init__(self, base_domain: str = "https://vietstock.vn"):
        self.base_domain = base_domain
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def is_article_from_today(self, pub_date_str: str) -> bool:
        """
        Check if article was published today (Vietnam timezone)
        
        Args:
            pub_date_str: Publication date string from RSS
            
        Returns:
            True if article was published today in Vietnam timezone
        """
        if not pub_date_str:
            return False
            
        try:
            # Get current date in Vietnam timezone (UTC+7)
            vietnam_tz = timezone(timedelta(hours=7))
            today_vietnam = datetime.now(vietnam_tz).date()
            
            # Parse the publication date
            pub_datetime = None
            
            # Try format with timezone: "Thu, 09 Oct 2025 23:04:20 +0700"
            try:
                pub_datetime = datetime.strptime(pub_date_str, "%a, %d %b %Y %H:%M:%S %z")
            except ValueError:
                pass
            
            # Try format without timezone: "Thu, 09 Oct 2025 23:04:20"
            if not pub_datetime:
                try:
                    pub_datetime = datetime.strptime(pub_date_str, "%a, %d %b %Y %H:%M:%S")
                    pub_datetime = pub_datetime.replace(tzinfo=vietnam_tz)
                except ValueError:
                    pass
            
            # Try ISO format
            if not pub_datetime:
                try:
                    pub_datetime = datetime.fromisoformat(pub_date_str.replace('Z', '+00:00'))
                except ValueError:
                    pass
            
            if pub_datetime:
                # Convert to Vietnam timezone for comparison
                pub_date_vietnam = pub_datetime.astimezone(vietnam_tz).date()
                return pub_date_vietnam == today_vietnam
            
            return False
            
        except Exception as e:
            logger.debug(f"Error parsing date {pub_date_str}: {e}")
            return False
    
    def get_rss_categories(self, rss_url: str) -> List[RSSCategory]:
        """
        Get RSS categories from Vietstock RSS page
        
        Args:
            rss_url: URL of the RSS page
            
        Returns:
            List of RSSCategory objects
        """
        logger.info(f"🔍 Getting RSS categories from: {rss_url}")
        
        try:
            response = self.session.get(rss_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            categories = []
            
            # Find RSS list containers
            rss_lists = soup.find_all('div', class_='list_item_rss')
            
            for rss_list in rss_lists:
                # Find all list items that contain main categories
                list_items = rss_list.find_all('li')
                
                for li in list_items:
                    # Check if this li contains a main category (direct a tag with .rss)
                    main_link = li.find('a', href=True)
                    if main_link:
                        href = main_link.get('href')
                        if href and '.rss' in href:
                            category_text = main_link.get_text(strip=True)
                            if category_text:
                                # Build correct RSS URL
                                full_url = urljoin(self.base_domain, str(href))
                                
                                # Get subcategories if any
                                subcategories = []
                                
                                # Look for subcategories in the nested ul with margin-left-20
                                sub_ul = li.find('ul', class_='margin-left-20')
                                if sub_ul:
                                    sub_links = sub_ul.find_all('a', href=True)
                                    for sub_link in sub_links:
                                        sub_href = sub_link.get('href')
                                        if sub_href and '.rss' in sub_href:
                                            sub_text = sub_link.get_text(strip=True)
                                            if sub_text:
                                                sub_full_url = urljoin(self.base_domain, str(sub_href))
                                                subcategories.append(RSSCategory(
                                                    name=sub_text,
                                                    url=sub_full_url
                                                ))
                                
                                categories.append(RSSCategory(
                                    name=category_text,
                                    url=full_url,
                                    subcategories=subcategories
                                ))
            
            # Remove duplicates by URL
            seen_urls = set()
            unique_categories = []
            for cat in categories:
                if cat.url not in seen_urls:
                    seen_urls.add(cat.url)
                    unique_categories.append(cat)
            
            categories = unique_categories
            logger.info(f"✅ Found {len(categories)} main categories")
            
            # Debug: Print first few categories
            for i, cat in enumerate(categories[:3]):
                logger.debug(f"  {i+1}. {cat.name}: {cat.url}")
                if cat.subcategories:
                    for j, sub in enumerate(cat.subcategories[:2]):
                        logger.debug(f"     - {sub.name}: {sub.url}")
            
            return categories
            
        except requests.RequestException as e:
            logger.error(f"❌ Network error getting categories: {e}")
            raise Exception(f"Network error: {e}")
        except Exception as e:
            logger.error(f"❌ Error parsing categories: {e}")
            raise Exception(f"Parse error: {e}")
    
    def parse_rss_feed(self, rss_url: str, category_name: str, filter_by_today: bool = True) -> List[Article]:
        """
        Parse RSS feed and extract articles
        
        Args:
            rss_url: RSS feed URL
            category_name: Name of the category
            filter_by_today: Whether to only return articles from today (Vietnam timezone)
                           If True, parsing will stop when first non-today article is found
                           since RSS feeds are chronological (newest first)
            
        Returns:
            List of Article objects
        """
        filter_info = " (today only)" if filter_by_today else ""
        logger.info(f"📡 Parsing RSS: {category_name}{filter_info} - {rss_url}")
        
        try:
            feed = feedparser.parse(rss_url)
            
            if feed.bozo:
                logger.warning(f"⚠️ RSS parsing warning for {category_name}: {feed.bozo_exception}")
            
            articles = []
            total_entries = len(feed.entries) if feed.entries else 0
            non_today_count = 0
            
            for i, entry in enumerate(feed.entries):
                pub_date_str = str(entry.get('published', ''))
                
                # Apply date filtering with early termination
                if filter_by_today and pub_date_str:
                    if not self.is_article_from_today(pub_date_str):
                        # RSS feeds are chronological (newest first), so if we find a non-today 
                        # article, all subsequent articles will also be non-today
                        remaining = total_entries - i
                        logger.info(f"📅 Found non-today article at position {i+1}/{total_entries}, stopping early ({remaining} articles skipped)")
                        break
                    else:
                        non_today_count += 1
                
                article = Article(
                    title=str(entry.get('title', '')),
                    link=str(entry.get('link', '')),
                    description=str(entry.get('description', '')),
                    pub_date=pub_date_str,
                    guid=str(entry.get('guid', entry.get('link', ''))),
                    category=category_name
                )
                
                # Clean description HTML tags
                if article.description:
                    soup_desc = BeautifulSoup(article.description, 'html.parser')
                    
                    # Extract image if present
                    img_tag = soup_desc.find('img')
                    if img_tag and img_tag.get('src'):
                        article.image = str(img_tag.get('src')) if img_tag.get('src') else None
                    
                    # Get text content
                    article.description_text = soup_desc.get_text(strip=True)
                
                articles.append(article)
            
            if filter_by_today:
                logger.info(f"✅ Found {len(articles)} articles from today (skipped {non_today_count} non-today)")
            else:
                logger.info(f"✅ Found {len(articles)} total articles")
                
            return articles
            
        except Exception as e:
            logger.error(f"❌ Error parsing RSS {rss_url}: {e}")
            return []
    
    def test_feed(self, feed_url: str) -> Dict[str, Any]:
        """
        Test RSS feed accessibility and parseability
        
        Args:
            feed_url: RSS feed URL to test
            
        Returns:
            Dictionary with test results
        """
        try:
            response = self.session.get(feed_url, timeout=10)
            response.raise_for_status()
            
            feed = feedparser.parse(feed_url)
            
            return {
                'accessible': True,
                'parseable': not feed.bozo,
                'entries_count': len(feed.entries) if feed.entries else 0,
                'title': getattr(feed.feed, 'title', None),
                'description': getattr(feed.feed, 'description', None),
                'error': None,
                'bozo_exception': str(feed.bozo_exception) if feed.bozo else None
            }
            
        except Exception as e:
            return {
                'accessible': False,
                'parseable': False,
                'entries_count': 0,
                'title': None,
                'description': None,
                'error': str(e),
                'bozo_exception': None
            }