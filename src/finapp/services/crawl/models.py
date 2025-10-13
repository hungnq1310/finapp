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
    # HTML content extraction fields
    raw_html: Optional[str] = None
    main_content: Optional[str] = None
    content_hash: Optional[str] = None
    html_extracted_at: Optional[str] = None
    html_extraction_success: bool = False
    
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
            'description_text': self.description_text,
            'raw_html': self.raw_html,
            'main_content': self.main_content,
            'content_hash': self.content_hash,
            'html_extracted_at': self.html_extracted_at,
            'html_extraction_success': self.html_extraction_success
        }
    
    def update_html_content(self, extraction_result: Dict[str, Any]) -> None:
        """
        Update article with HTML extraction results
        
        Args:
            extraction_result: Result from HTMLContentExtractor.extract_article_content()
        """
        if extraction_result.get('extraction_success', False):
            self.raw_html = extraction_result.get('raw_html')
            self.main_content = extraction_result.get('main_content')
            self.content_hash = extraction_result.get('content_hash')
            self.html_extracted_at = extraction_result.get('extracted_at')
            self.html_extraction_success = True
        else:
            self.html_extraction_success = False


@dataclass
class CrawlSession:
    """Crawl session model"""
    crawled_at: str = field(default_factory=lambda: datetime.now().isoformat())
    source: str = "vietstock.vn"
    base_url: str = ""
    output_directory: str = ""
    categories: List[Dict[str, Any]] = field(default_factory=list)
    total_articles: int = 0
    # HTML extraction fields
    html_extraction_results: Optional[Dict[str, Any]] = None
    html_extraction_error: Optional[str] = None
    html_extraction_enabled: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'crawled_at': self.crawled_at,
            'source': self.source,
            'base_url': self.base_url,
            'output_directory': self.output_directory,
            'categories': self.categories,
            'total_articles': self.total_articles,
            'html_extraction_results': self.html_extraction_results,
            'html_extraction_error': self.html_extraction_error,
            'html_extraction_enabled': self.html_extraction_enabled
        }