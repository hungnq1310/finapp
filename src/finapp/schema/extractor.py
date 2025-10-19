"""
Comprehensive Pydantic Models for LLM Financial News Extractor

This module defines detailed models for 4-level financial news analysis.
"""

from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field


class SentimentAnalysis(BaseModel):
    """Sentiment analysis result"""
    overall_sentiment: str = Field(..., description="Overall sentiment: tích cực/tiêu cực/trung lập")
    sentiment_score: float = Field(..., ge=-1, le=1, description="Sentiment score from -1 to 1")
    key_factors: List[str] = Field(default_factory=list, description="Key factors affecting sentiment")


class StockLevel(BaseModel):
    """Stock-level analysis"""
    ticker: str = Field(..., pattern=r"^[A-Z]{2,4}$", description="Stock ticker symbol")
    company_name: Optional[str] = Field(None, description="Company name")
    sentiment: str = Field(..., description="Stock sentiment: tích cực/tiêu cực/trung lập")
    impact_type: str = Field(..., description="Type of impact: tài chính/hoạt động kinh doanh/thị trường/quản trị/pháp lý/khác")
    price_impact: str = Field(..., description="Expected price impact: tăng/giảm/không đổi/chưa xác định")
    confidence: float = Field(..., ge=0, le=1, description="Confidence score")


class SectorLevel(BaseModel):
    """Sector-level analysis"""
    sector_name: str = Field(..., description="Sector name from predefined list")
    sentiment: str = Field(..., description="Sector sentiment: tích cực/tiêu cực/trung lập")
    impact_description: str = Field(..., description="Description of sector impact")
    affected_companies: List[str] = Field(default_factory=list, description="List of affected companies")


class MarketLevel(BaseModel):
    """Market-level analysis"""
    scope: str = Field(..., description="Impact scope: toàn thị trường/ngành cụ thể/cổ phiếu cụ thể/chưa xác định")
    exchange: str = Field(..., description="Exchange: HOSE/HNX/UPCOM/cả ba/chưa xác định")
    market_moving: bool = Field(..., description="Whether this news will move the market")
    impact_magnitude: Optional[str] = Field(None, description="Impact magnitude: cao/trung bình/thấp/không ảnh hưởng")
    key_indices: List[str] = Field(default_factory=list, description="Affected indices: VN-Index/HNX-Index/VN30/etc")


class FinancialData(BaseModel):
    """Financial numbers and data"""
    has_numbers: bool = Field(..., description="Whether article contains financial numbers")
    revenues: List[Dict[str, Any]] = Field(default_factory=list, description="Revenue figures")
    profits: List[Dict[str, Any]] = Field(default_factory=list, description="Profit figures")
    percentages: List[Dict[str, Any]] = Field(default_factory=list, description="Percentage figures")
    amounts: List[Dict[str, Any]] = Field(default_factory=list, description="Other monetary amounts")


class FinancialNewsExtraction(BaseModel):
    """Comprehensive extraction result with 4-level analysis"""
    sentiment_analysis: SentimentAnalysis
    stock_level: List[StockLevel] = Field(default_factory=list)
    sector_level: List[SectorLevel] = Field(default_factory=list)
    market_level: MarketLevel
    financial_data: FinancialData
    
    # Article metadata
    article_guid: str = Field(..., description="Unique identifier for the article")
    article_title: str = Field(..., description="Article title")
    article_category: str = Field(..., description="Article category")
    extraction_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="When extraction was performed")
    extraction_model: str = Field(..., description="LLM model used for extraction")
    extraction_confidence: Optional[float] = Field(None, ge=0, le=1, description="Confidence in extraction results")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ExtractionBatchResult(BaseModel):
    """Results for batch extraction of multiple articles"""
    total_articles: int = Field(..., description="Total number of articles processed")
    successful_extractions: int = Field(..., description="Number of successful extractions")
    failed_extractions: int = Field(..., description="Number of failed extractions")
    extraction_time_seconds: float = Field(..., description="Total time taken for extraction")
    results: List[FinancialNewsExtraction] = Field(..., description="Individual extraction results")
    errors: List[str] = Field(default_factory=list, description="Error messages for failed extractions")
    batch_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="When batch processing was performed")
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage"""
        if self.total_articles == 0:
            return 0.0
        return (self.successful_extractions / self.total_articles) * 100


class ExtractionSession(BaseModel):
    """Session tracking for extraction operations"""
    session_id: str = Field(..., description="Unique session identifier")
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    status: str = Field(default="running", description="Session status: running, completed, failed")
    total_batches: int = Field(default=0, description="Number of batches in session")
    completed_batches: int = Field(default=0, description="Number of completed batches")
    total_articles: int = Field(default=0, description="Total articles in session")
    processed_articles: int = Field(default=0, description="Total processed articles")
    successful_extractions: int = Field(default=0, description="Total successful extractions")
    failed_extractions: int = Field(default=0, description="Total failed extractions")
    errors: List[str] = Field(default_factory=list, description="Session-level errors")
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress as percentage"""
        if self.total_batches == 0:
            return 0.0
        return (self.completed_batches / self.total_batches) * 100
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage"""
        if self.processed_articles == 0:
            return 0.0
        return (self.successful_extractions / self.processed_articles) * 100


# Update the request.py file to include proper response models
class LLMExtractorResponse(BaseModel):
    """Response model for LLM Extractor API"""
    success: bool = Field(..., description="Whether extraction was successful")
    message: str = Field(..., description="Response message")
    data: Optional[Dict[str, Any]] = Field(None, description="Response data")
    session_id: Optional[str] = Field(None, description="Session ID for tracking")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")