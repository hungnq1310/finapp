"""
Pydantic Models for Financial News Analysis

This module defines data models for stock reports and market data.
"""

from typing import List, Optional
from pydantic import BaseModel


class IndexSummary(BaseModel):
    """Model for index summary data"""
    allQty: float
    change: float
    indexId: str
    advances: int
    allValue: float
    declines: int
    nochanges: int
    indexValue: float
    changePercent: float
    gtdg_last_day: float
    klgd_last_day: float
    gtdg_change_amount: float
    klgd_change_amount: float
    gtdg_change_percent: float
    klgd_change_percent: float


class ImpactData(BaseModel):
    """Model for impact data (up/down)"""
    total: float
    stock_code: List[str]


class KhoiNgoaiData(BaseModel):
    """Model for foreign investor data"""
    vol: float
    net_value: float


class NetForeignData(BaseModel):
    """Model for net foreign trading data"""
    buy: List[str]
    sell: List[str]


class DataQuality(BaseModel):
    """Model for data quality information"""
    issues: List[str]
    passed: bool
    warnings: List[str]
    timestamp: str


class LastDayComparison(BaseModel):
    """Model for last day comparison metadata"""
    source_file: str
    trading_date: str
    has_comparison_data: bool


class ExecutionMetadata(BaseModel):
    """Model for execution metadata"""
    errors: List[str]
    timestamp: str
    success_rate: float
    total_modules: int
    successful_modules: int
    last_day_comparison: LastDayComparison


class ExecutionSummary(BaseModel):
    """Model for execution summary"""
    api_health: bool
    saved_to_minio: bool
    input_validation: bool
    data_merge_success: float
    data_quality_passed: bool


class PerformanceMetrics(BaseModel):
    """Model for performance metrics"""
    data_success_rate: int
    successful_modules: int
    api_health_percentage: int
    total_modules_executed: int


class StorageInfo(BaseModel):
    """Model for MinIO storage information"""
    type: str
    filename: str
    location: str
    size_bytes: int
    download_url: str


class OutputData(BaseModel):
    """Model for the main output data"""
    impact_up: ImpactData
    khoi_ngoai: KhoiNgoaiData
    impact_down: ImpactData
    top_sectors: List[str]
    data_quality: DataQuality
    index_summary: List[IndexSummary]
    khoi_tu_doanh: float
    top_interested: List[str]
    top_netforeign: NetForeignData
    execution_metadata: ExecutionMetadata


class IndexReport(BaseModel):
    """Complete index report model"""
    storage: StorageInfo
    timestamp: str
    output_data: OutputData
    recommendations: List[str]
    workflow_status: str
    execution_summary: ExecutionSummary
    performance_metrics: PerformanceMetrics


class IndexReportListItem(BaseModel):
    """Model for index report list items"""
    filename: str
    timestamp: str
    size_bytes: int
    last_modified: Optional[str] = None
    workflow_status: Optional[str] = None


class IndexReportListResponse(BaseModel):
    """Response model for listing index reports"""
    reports: List[IndexReportListItem]
    total_count: int
    has_more: bool = False