"""
Data Models for Financial News Analysis System

This module defines the core data structures using OOP patterns.
All models follow simple, clear design principles.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum


# Enums for type safety
class ProcessingStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class SourceType(Enum):
    NEWS = "news"
    SEC_FILING = "sec_filing"
    EARNINGS_RELEASE = "earnings_release"


class MarketSession(Enum):
    PRE_MARKET = "pre_market"
    MARKET_HOURS = "market_hours"
    AFTER_HOURS = "after_hours"
    CLOSED = "closed"


class EventType(Enum):
    EARNINGS_BEAT = "earnings_beat"
    EARNINGS_MISS = "earnings_miss"
    GUIDANCE_RAISE = "guidance_raise"
    GUIDANCE_LOWER = "guidance_lower"
    DIVIDEND_INCREASE = "dividend_increase"
    MERGER_ANNOUNCED = "merger_announced"
    CEO_CHANGE = "ceo_change"


class RecommendationAction(Enum):
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    HOLD = "hold"
    SELL = "sell"
    STRONG_SELL = "strong_sell"


# Base Document class
@dataclass
class BaseDocument:
    """Base class for all MongoDB documents"""
    id: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage"""
        return {
            "_id": self.id,
            **{k: v for k, v in self.__dict__.items() if k != 'id'}
        }


# Content Models
@dataclass
class Source:
    """Source information for articles"""
    url: str
    domain: str
    source_type: SourceType
    name: Optional[str] = None
    credibility_score: Optional[float] = None


@dataclass
class Content:
    """Article content structure"""
    headline: str
    summary: str
    body: str
    subheadline: Optional[str] = None
    author: Optional[str] = None


@dataclass
class CompanyMention:
    """Information about company mentioned in article"""
    company_id: str
    ticker: str
    company_name: str
    relevance_score: float  # 0-1
    mention_type: str  # primary_subject, secondary_subject, mentioned
    sentiment: float  # -1 to 1
    context: str


@dataclass
class ExtractedEvent:
    """Financial event extracted from news"""
    event_type: EventType
    description: str
    companies_affected: List[str]
    impact_magnitude: Optional[float] = None
    confidence: float = 0.0


@dataclass
class Sentiment:
    """Sentiment analysis results"""
    overall_sentiment: float  # -1 to 1
    sentiment_magnitude: float  # 0 to 1
    market_impact_score: float  # 0 to 1
    emotional_tone: Dict[str, float] = field(default_factory=dict)


# Main Document Classes - Fixed field ordering
class RawDocument(BaseDocument):
    """Raw HTML/PDF document before processing"""
    
    def __init__(self, id: str, source: Source, content_html: str, 
                 content_text: str, metadata: Dict[str, Any],
                 processing_status: ProcessingStatus = ProcessingStatus.PENDING,
                 retry_count: int = 0, error_message: Optional[str] = None,
                 created_at: Optional[datetime] = None):
        super().__init__(id, created_at or datetime.utcnow())
        self.source = source
        self.content_html = content_html
        self.content_text = content_text
        self.metadata = metadata
        self.processing_status = processing_status
        self.retry_count = retry_count
        self.error_message = error_message
    
    def mark_processing(self):
        """Mark document as being processed"""
        self.processing_status = ProcessingStatus.PROCESSING
    
    def mark_completed(self):
        """Mark document as successfully processed"""
        self.processing_status = ProcessingStatus.COMPLETED
    
    def mark_failed(self, error: str):
        """Mark document as failed with error message"""
        self.processing_status = ProcessingStatus.FAILED
        self.error_message = error


class NewsArticle(BaseDocument):
    """Processed news article with structured data"""
    
    def __init__(self, id: str, raw_document_id: str, content: Content,
                 source: Source, published_at: datetime, market_session: MarketSession,
                 companies_mentioned: List[CompanyMention], events_extracted: List[ExtractedEvent],
                 sentiment: Sentiment, classification: Dict[str, Any],
                 extraction_confidence: float = 0.0, created_at: Optional[datetime] = None):
        super().__init__(id, created_at or datetime.utcnow())
        self.raw_document_id = raw_document_id
        self.content = content
        self.source = source
        self.published_at = published_at
        self.market_session = market_session
        self.companies_mentioned = companies_mentioned
        self.events_extracted = events_extracted
        self.sentiment = sentiment
        self.classification = classification
        self.extraction_confidence = extraction_confidence
    
    def get_primary_companies(self) -> List[CompanyMention]:
        """Get companies that are primary subjects"""
        return [c for c in self.companies_mentioned 
                if c.mention_type == "primary_subject"]
    
    def get_positive_sentiment_companies(self) -> List[CompanyMention]:
        """Get companies with positive sentiment"""
        return [c for c in self.companies_mentioned if c.sentiment > 0.1]


@dataclass
class ReportMetadata:
    """Metadata for analysis reports"""
    report_type: str
    analysis_period_start: datetime
    analysis_period_end: datetime
    generated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class KeyDevelopment:
    """Key development in stock analysis"""
    category: str
    title: str
    description: str
    impact: str  # positive, negative, neutral
    importance: str  # high, medium, low


@dataclass
class Recommendation:
    """Investment recommendation"""
    action: RecommendationAction
    confidence: float
    reasoning: str


class StockReport(BaseDocument):
    """Stock analysis report"""
    
    def __init__(self, id: str, company_id: str, ticker: str,
                 metadata: ReportMetadata, news_count: int, average_sentiment: float,
                 key_developments: List[KeyDevelopment], recommendation: Recommendation,
                 executive_summary: str, outlook: Dict[str, str], risks: List[str],
                 created_at: Optional[datetime] = None):
        super().__init__(id, created_at or datetime.utcnow())
        self.company_id = company_id
        self.ticker = ticker
        self.metadata = metadata
        self.news_count = news_count
        self.average_sentiment = average_sentiment
        self.key_developments = key_developments
        self.recommendation = recommendation
        self.executive_summary = executive_summary
        self.outlook = outlook
        self.risks = risks
    
    def is_bullish(self) -> bool:
        """Check if recommendation is bullish"""
        return self.recommendation.action in [
            RecommendationAction.BUY, 
            RecommendationAction.STRONG_BUY
        ]


@dataclass
class SectorInfo:
    """Sector classification information"""
    gics_code: str
    sector_name: str
    sub_sector: Optional[str] = None


class SectorReport(BaseDocument):
    """Sector analysis report"""
    
    def __init__(self, id: str, sector: SectorInfo, metadata: ReportMetadata,
                 total_companies: int, companies_with_news: int, average_sentiment: float,
                 executive_summary: str, key_trends: List[Dict[str, Any]], outlook: str,
                 risks: List[str], opportunities: List[str], created_at: Optional[datetime] = None):
        super().__init__(id, created_at or datetime.utcnow())
        self.sector = sector
        self.metadata = metadata
        self.total_companies = total_companies
        self.companies_with_news = companies_with_news
        self.average_sentiment = average_sentiment
        self.executive_summary = executive_summary
        self.key_trends = key_trends
        self.outlook = outlook
        self.risks = risks
        self.opportunities = opportunities


class MarketReport(BaseDocument):
    """Overall market analysis report"""
    
    def __init__(self, id: str, metadata: ReportMetadata, total_news_articles: int,
                 companies_covered: int, sectors_analyzed: int, overall_sentiment: float,
                 executive_summary: str, market_themes: List[Dict[str, Any]],
                 sector_performance: List[Dict[str, Any]], outlook: Dict[str, str],
                 trading_strategy: str, created_at: Optional[datetime] = None):
        super().__init__(id, created_at or datetime.utcnow())
        self.metadata = metadata
        self.total_news_articles = total_news_articles
        self.companies_covered = companies_covered
        self.sectors_analyzed = sectors_analyzed
        self.overall_sentiment = overall_sentiment
        self.executive_summary = executive_summary
        self.market_themes = market_themes
        self.sector_performance = sector_performance
        self.outlook = outlook
        self.trading_strategy = trading_strategy
