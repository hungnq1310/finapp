"""
Data models for Vietstock crawler
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field


@dataclass
class RSSCategory:
    """RSS Category model"""
    name: str
    url: str
    subcategories: List['RSSCategory'] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'name': self.name,
            'url': self.url,
            'subcategories': [subcat.to_dict() for subcat in self.subcategories]
        }


@dataclass
class Article:
    """Article model"""
    title: str
    link: str
    description: str = ""
    pub_date: str = ""
    guid: str = ""
    category: str = ""
    source: str = "vietstock"
    crawled_at: str = field(default_factory=lambda: datetime.now().isoformat())
    image: Optional[str] = None
    description_text: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'title': self.title,
            'link': self.link,
            'description': self.description,
            'pub_date': self.pub_date,
            'guid': self.guid,
            'category': self.category,
            'source': self.source,
            'crawled_at': self.crawled_at,
            'image': self.image,
            'description_text': self.description_text
        }


@dataclass
class CrawlSession:
    """Crawl session model"""
    crawled_at: str = field(default_factory=lambda: datetime.now().isoformat())
    source: str = "vietstock.vn"
    base_url: str = ""
    output_directory: str = ""
    categories: List[Dict[str, Any]] = field(default_factory=list)
    total_articles: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'crawled_at': self.crawled_at,
            'source': self.source,
            'base_url': self.base_url,
            'output_directory': self.output_directory,
            'categories': self.categories,
            'total_articles': self.total_articles
        }