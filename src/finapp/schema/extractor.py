"""
Pydantic Models for LLM Financial News Extractor

This module defines structured models for financial news analysis output,
based on the JSON schema defined in json-schema/extractor.json.
"""

from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field


class SentimentAnalysis(BaseModel):
    """Sentiment analysis results"""
    overall_sentiment: str = Field(..., description="Phân loại cảm xúc tổng thể của bài báo")
    sentiment_score: float = Field(..., ge=-1, le=1, description="Điểm sentiment từ -1 (rất tiêu cực) đến 1 (rất tích cực)")
    market_sentiment: str = Field(..., description="Tâm lý thị trường: tăng giá, giảm giá, hoặc trung tính")


class StockTicker(BaseModel):
    """Stock ticker information"""
    ticker: str = Field(..., pattern=r"^[A-Z]{3,4}$", description="Mã cổ phiếu (VD: VCB, FPT, HPG)")
    exchange: Optional[str] = Field(None, description="Sàn giao dịch")
    sentiment: str = Field(..., description="Phân loại cảm xúc đối với mã cổ phiếu này")
    relevance_score: float = Field(..., ge=0, le=1, description="Mức độ liên quan của bài báo đến mã cổ phiếu này")


class SectorIndustry(BaseModel):
    """Sector/industry classification"""
    sector_name: str = Field(..., description="Tên ngành/lĩnh vực")
    sentiment: str = Field(..., description="Tình cảm đối với ngành/lĩnh vực này")
    impact_level: str = Field(..., description="Mức độ tác động của tin tức đến ngành này")


class FinancialIndicators(BaseModel):
    """Financial indicators mentioned in article"""
    mentions_price: bool = Field(..., description="Có đề cập đến giá cổ phiếu không")
    price_direction: str = Field(..., description="Hướng biến động giá được đề cập")
    mentions_volume: bool = Field(..., description="Có đề cập đến khối lượng giao dịch không")
    mentions_earnings: bool = Field(..., description="Có đề cập đến lợi nhuận/kết quả kinh doanh không")
    mentions_revenue: bool = Field(..., description="Có đề cập đến doanh thu không")
    mentions_dividends: bool = Field(..., description="Có đề cập đến cổ tức không")
    financial_metrics: List[str] = Field(default_factory=list, description="Các chỉ số tài chính được đề cập")


class MarketImpact(BaseModel):
    """Market impact assessment"""
    impact_scope: str = Field(..., description="Phạm vi tác động của tin tức")
    time_horizon: str = Field(..., description="Khoảng thời gian tác động dự kiến")
    market_moving_news: bool = Field(..., description="Có phải tin tức có thể làm dao động thị trường không")


class NewsClassification(BaseModel):
    """News classification details"""
    news_type: str = Field(..., description="Loại tin tức")
    urgency: str = Field(..., description="Mức độ khẩn cấp/quan trọng của tin tức")
    reliability_score: float = Field(..., ge=0, le=1, description="Điểm tin cậy của thông tin")


class KeyEvent(BaseModel):
    """Key events mentioned in article"""
    event_type: str = Field(..., description="Loại sự kiện")
    expected_date: Optional[str] = Field(None, description="Ngày dự kiến sự kiện (YYYY-MM-DD format)")
    impact_assessment: str = Field(..., description="Đánh giá tác động của sự kiện")


class NumericalData(BaseModel):
    """Numerical data extracted from article"""
    has_specific_numbers: bool = Field(..., description="Có chứa số liệu cụ thể không")
    currency_amounts: List[Dict[str, Any]] = Field(default_factory=list, description="Các con số về tiền tệ được đề cập")
    percentages: List[Dict[str, Any]] = Field(default_factory=list, description="Các tỷ lệ phần trăm được đề cập")


class FinancialNewsExtraction(BaseModel):
    """Complete extraction result for a financial news article"""
    sentiment_analysis: SentimentAnalysis
    stock_tickers: List[StockTicker] = Field(default_factory=list)
    sectors_industries: List[SectorIndustry] = Field(default_factory=list)
    financial_indicators: FinancialIndicators
    market_impact: MarketImpact
    news_classification: NewsClassification
    key_events: List[KeyEvent] = Field(default_factory=list)
    risk_factors: List[str] = Field(default_factory=list)
    numerical_data: NumericalData
    
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