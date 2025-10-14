"""
Vietstock-specific schema for RSS crawler system

This module defines data models for Vietstock RSS articles that are compatible
with the MongoDB-based architecture of the main branch.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum

from .base import BaseDocument, Source, Content


class VietstockCategory(Enum):
    """Vietstock RSS categories"""
    STOCK_NEWS = "tintuc-chungkhoan"
    COMPANY_NEWS = "tintuc-doanhnghiep"
    WORLD_MARKETS = "tintuc-thegioi"
    FOREX = "tintuc-forex"
    COMMODITIES = "tintuc-hanghoa"
    ANALYSIS = "phan-tich-chungkhoan"
    TECHNICAL = "phan-tich-kythuat"
    FUNDAMENTAL = "phan-tich-coban"
    MARKET_OUTLOOK = "nhan-dinh-thi-truong"
    IPO = "tintuc-ipo"
    DERIVATIVES = "tintuc-phaisinh"
    BONDS = "tintuc-trai-phieu"


@dataclass
class VietstockSource(Source):
    """Vietstock-specific source information"""
    rss_url: Optional[str] = None
    category: Optional[str] = None
    
    def __init__(self, url: str, rss_url: str = None, category: str = None):
        super().__init__(
            url=url,
            domain="vietstock.vn",
            source_type=SourceType.NEWS,
            name="Vietstock",
            credibility_score=0.85
        )
        self.rss_url = rss_url
        self.category = category


@dataclass
class VietstockContent(Content):
    """Vietstock article content with RSS-specific fields"""
    rss_description: Optional[str] = None
    rss_guid: Optional[str] = None
    rss_pub_date: Optional[str] = None
    image_url: Optional[str] = None
    description_text: Optional[str] = None
    
    # HTML extraction fields
    raw_html: Optional[str] = None
    main_content: Optional[str] = None
    content_hash: Optional[str] = None
    html_extracted_at: Optional[datetime] = None
    html_extraction_success: bool = False


class VietstockArticle(BaseDocument):
    """Vietstock RSS article model compatible with MongoDB architecture"""
    
    def __init__(
        self,
        id: str,
        source: VietstockSource,
        content: VietstockContent,
        published_at: datetime,
        rss_category: str,
        crawled_at: Optional[datetime] = None,
        extraction_confidence: float = 0.0
    ):
        super().__init__(id, crawled_at or datetime.utcnow())
        self.source = source
        self.content = content
        self.published_at = published_at
        self.rss_category = rss_category
        self.extraction_confidence = extraction_confidence
    
    def get_rss_guid(self) -> str:
        """Get RSS GUID for deduplication"""
        return self.content.rss_guid or self.id
    
    def is_html_extracted(self) -> bool:
        """Check if HTML content has been extracted"""
        return self.content.html_extraction_success
    
    def update_html_content(self, extraction_result: Dict[str, Any]) -> None:
        """
        Update article with HTML extraction results
        
        Args:
            extraction_result: Result from HTML content extraction
        """
        if extraction_result.get('extraction_success', False):
            self.content.raw_html = extraction_result.get('raw_html')
            self.content.main_content = extraction_result.get('main_content')
            self.content.content_hash = extraction_result.get('content_hash')
            self.content.html_extracted_at = extraction_result.get('extracted_at')
            self.content.html_extraction_success = True
            self.extraction_confidence = extraction_result.get('confidence', 0.0)
        else:
            self.content.html_extraction_success = False
            self.extraction_confidence = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage"""
        base_dict = super().to_dict()
        base_dict.update({
            'source': {
                'url': self.source.url,
                'domain': self.source.domain,
                'source_type': self.source.source_type.value,
                'name': self.source.name,
                'credibility_score': self.source.credibility_score,
                'rss_url': self.source.rss_url,
                'category': self.source.category
            },
            'content': {
                'headline': self.content.headline,
                'summary': self.content.summary,
                'body': self.content.body,
                'subheadline': self.content.subheadline,
                'author': self.content.author,
                'rss_description': self.content.rss_description,
                'rss_guid': self.content.rss_guid,
                'rss_pub_date': self.content.rss_pub_date,
                'image_url': self.content.image_url,
                'description_text': self.content.description_text,
                'raw_html': self.content.raw_html,
                'main_content': self.content.main_content,
                'content_hash': self.content.content_hash,
                'html_extracted_at': self.content.html_extracted_at.isoformat() if self.content.html_extracted_at else None,
                'html_extraction_success': self.content.html_extraction_success
            },
            'published_at': self.published_at.isoformat(),
            'rss_category': self.rss_category,
            'extraction_confidence': self.extraction_confidence
        })
        return base_dict


@dataclass
class VietstockCrawlSession(BaseDocument):
    """Vietstock crawl session tracking"""
    
    def __init__(
        self,
        id: str,
        source_base_url: str,
        categories_crawled: List[str],
        total_articles_found: int,
        new_articles_saved: int,
        html_extraction_enabled: bool = False,
        html_extraction_stats: Optional[Dict[str, Any]] = None,
        duration_seconds: float = 0.0,
        success: bool = True,
        error_message: Optional[str] = None,
        created_at: Optional[datetime] = None
    ):
        super().__init__(id, created_at or datetime.utcnow())
        self.source_base_url = source_base_url
        self.categories_crawled = categories_crawled
        self.total_articles_found = total_articles_found
        self.new_articles_saved = new_articles_saved
        self.html_extraction_enabled = html_extraction_enabled
        self.html_extraction_stats = html_extraction_stats or {}
        self.duration_seconds = duration_seconds
        self.success = success
        self.error_message = error_message
    
    def get_success_rate(self) -> float:
        """Get success rate of article processing"""
        if self.total_articles_found == 0:
            return 0.0
        return self.new_articles_saved / self.total_articles_found
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage"""
        base_dict = super().to_dict()
        base_dict.update({
            'source_base_url': self.source_base_url,
            'categories_crawled': self.categories_crawled,
            'total_articles_found': self.total_articles_found,
            'new_articles_saved': self.new_articles_saved,
            'html_extraction_enabled': self.html_extraction_enabled,
            'html_extraction_stats': self.html_extraction_stats,
            'duration_seconds': self.duration_seconds,
            'success_rate': self.get_success_rate(),
            'success': self.success,
            'error_message': self.error_message
        })
        return base_dict


@dataclass
class RSSCategoryInfo:
    """RSS Category information for Vietstock"""
    name: str
    url: str
    subcategories: List['RSSCategoryInfo'] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'name': self.name,
            'url': self.url,
            'subcategories': [subcat.to_dict() for subcat in self.subcategories]
        }


# Import SourceType for compatibility
from .base import SourceType