"""
Configuration Data Classes for Financial News System

This module provides dataclasses for configuring different components
of the financial news system with support for multiple sources.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any
from pathlib import Path
import os


@dataclass
class SourceConfig:
    """Configuration for a news source"""
    name: str
    base_url: str
    base_domain: str
    rss_url: Optional[str] = None
    output_dir: Optional[str] = None
    source_type: str = "rss"  # rss, web, api
    
    # Parser specific settings
    title_selector: str = "title"
    link_selector: str = "link"
    description_selector: str = "description"
    pub_date_selector: str = "pubDate"
    guid_selector: str = "guid"
    
    # Content extraction settings
    content_extractors: List[str] = field(default_factory=lambda: [
        "newspaper.Article",
        "trafilatura.fetch_url"
    ])
    
    # Filtering settings
    date_format: str = "%a, %d %b %Y %H:%M:%S %z"
    timezone_offset: int = 7  # Vietnam timezone UTC+7
    
    # Rate limiting
    rate_limit_delay: float = 1.0
    max_articles_per_request: int = 100
    
    # Custom settings
    custom_settings: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StorageConfig:
    """Configuration for storage backend"""
    storage_type: str = field(default_factory=lambda: os.getenv("STORAGE_TYPE", "hybrid"))
    
    # JSON storage settings
    base_dir: str = field(default_factory=lambda: os.getenv("STORAGE_BASE_DIR", "data"))
    file_format: str = "json"
    compression: bool = field(default_factory=lambda: os.getenv("STORAGE_COMPRESSION", "false").lower() == "true")
    
    # MongoDB settings
    mongodb_uri: str = field(default_factory=lambda: os.getenv("MONGODB_URI", ""))
    database_name: str = field(default_factory=lambda: os.getenv("DATABASE_NAME", ""))
    
    # Hybrid settings
    use_mongodb_primary: bool = False
    local_backup: bool = True
    
    # Retention settings
    retention_days: int = 30
    archive_old_data: bool = True


@dataclass
class LLMConfig:
    """Configuration for LLM extractor"""
    provider: str = "openrouter"  # openrouter, openai, anthropic
    
    # API settings
    api_key: str = ""
    base_url: str = ""
    model_name: str = ""
    temperature: float = 0.65
    max_tokens: int = 4096
    
    # Prompt settings
    prompts_dir: str = "prompts"
    template_name: str = "extractor.j2"
    language: str = "vietnamese"
    
    # JSON schema settings
    schema_dir: str = "json-schema"
    schema_file: str = "extractor.json"
    
    # Extraction settings
    batch_size: int = 5
    delay_seconds: float = 1.0
    max_retries: int = 3
    timeout_seconds: int = 60
    
    # Confidence settings
    min_confidence_threshold: float = 0.5
    require_confidence: bool = True


@dataclass
class MasterJSONConfig:
    """Configuration for master JSON service"""
    base_dir: str = field(default_factory=lambda: os.getenv("MASTER_JSON_DIR", "data/master"))
    organization_type: str = "date_hierarchy"  # date_hierarchy, flat
    
    # File settings
    file_prefix: str = "master_"
    file_format: str = "json"
    include_full_content: bool = True
    auto_backup: bool = True
    
    # Index settings
    build_indexes: bool = True
    index_types: List[str] = field(default_factory=lambda: [
        "by_ticker", "by_sector", "by_sentiment", "by_time"
    ])
    
    # Summary settings
    auto_generate_summary: bool = True
    top_n_stocks: int = 10
    top_n_sectors: int = 10


@dataclass
class CrawlerConfig:
    """Configuration for crawler service"""
    sources: List[SourceConfig] = field(default_factory=list)
    storage: StorageConfig = field(default_factory=StorageConfig)
    
    # Scheduler settings
    enable_scheduler: bool = True
    interval_minutes: int = 60
    auto_start: bool = False
    
    # Filtering settings
    date_filter_enabled: bool = True
    timezone: str = "Asia/Ho_Chi_Minh"
    min_confidence_score: float = 0.3
    
    # Content extraction settings
    extract_html_content: bool = True
    html_extraction_methods: List[str] = field(default_factory=lambda: [
        "newspaper3k", "trafilatura"
    ])
    
    # Rate limiting
    global_rate_limit: float = 1.0
    per_source_rate_limits: Dict[str, float] = field(default_factory=dict)


@dataclass
class ExtractorConfig:
    """Configuration for extraction service"""
    llm: LLMConfig = field(default_factory=LLMConfig)
    master_json: MasterJSONConfig = field(default_factory=MasterJSONConfig)
    
    # Service settings
    enable_session_management: bool = True
    session_timeout_minutes: int = 120
    
    # Processing settings
    auto_organize_results: bool = True
    parallel_processing: bool = False
    max_workers: int = 4
    
    # Quality control
    validation_enabled: bool = True
    quality_threshold: float = 0.7
    auto_retry_failed: bool = True


@dataclass
class APIConfig:
    """Configuration for API service"""
    host: str = "0.0.0.0"
    port: int = 8002
    reload: bool = True
    debug: bool = False
    
    # CORS settings
    cors_origins: List[str] = field(default_factory=lambda: ["*"])
    cors_methods: List[str] = field(default_factory=lambda: ["*"])
    cors_headers: List[str] = field(default_factory=lambda: ["*"])
    
    # Rate limiting
    enable_rate_limiting: bool = False
    requests_per_minute: int = 100
    
    # Auth settings
    enable_auth: bool = False
    jwt_secret: str = ""
    jwt_algorithm: str = "HS256"
    
    # Logging
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


@dataclass
class SystemConfig:
    """Main configuration for the entire system"""
    crawler: CrawlerConfig = field(default_factory=CrawlerConfig)
    extractor: ExtractorConfig = field(default_factory=ExtractorConfig)
    api: APIConfig = field(default_factory=APIConfig)
    
    # System-wide settings
    environment: str = "development"  # development, staging, production
    debug_mode: bool = True
    
    # Multi-source settings
    enabled_sources: List[str] = field(default_factory=lambda: ["vietstock"])
    default_source: str = "vietstock"
    
    # Performance settings
    cache_enabled: bool = True
    cache_ttl_seconds: int = 300
    
    # Monitoring
    enable_metrics: bool = False
    metrics_port: int = 9090


# Predefined source configurations
# All URLs and paths now use environment variables

VIETSTOCK_CONFIG = SourceConfig(
    name="vietstock",
    base_url=os.getenv("VIETSTOCK_BASE_URL", "https://vietstock.vn/rss"),
    base_domain=os.getenv("VIETSTOCK_BASE_DOMAIN", "https://vietstock.vn"),
    rss_url=os.getenv("VIETSTOCK_RSS_URL", "https://vietstock.vn/rss"),
    output_dir=os.getenv("VIETSTOCK_OUTPUT_DIR", "data/vietstock"),
    custom_settings={
        "vietnamese_content": True,
        "financial_focus": True
    }
)

CAFEF_CONFIG = SourceConfig(
    name="cafef",
    base_url=os.getenv("CAFEF_BASE_URL", "https://cafef.vn/rss"),
    base_domain=os.getenv("CAFEF_BASE_DOMAIN", "https://cafef.vn"),
    rss_url=os.getenv("CAFEF_RSS_URL", "https://cafef.vn/rss"),
    output_dir=os.getenv("CAFEF_OUTPUT_DIR", "data/cafef"),
    title_selector="title",
    link_selector="link",
    description_selector="description",
    pub_date_selector="pubDate",
    custom_settings={
        "vietnamese_content": True,
        "financial_focus": True,
        "market_data_focus": True
    }
)

VIETNAMFINANCE_CONFIG = SourceConfig(
    name="vietnamfinance",
    base_url=os.getenv("VIETNAMFINANCE_BASE_URL", "https://vietnamfinance.vn/rss"),
    base_domain=os.getenv("VIETNAMFINANCE_BASE_DOMAIN", "https://vietnamfinance.vn"),
    rss_url=os.getenv("VIETNAMFINANCE_RSS_URL", "https://vietnamfinance.vn/rss"),
    output_dir=os.getenv("VIETNAMFINANCE_OUTPUT_DIR", "data/vietnamfinance"),
    custom_settings={
        "vietnamese_content": True,
        "financial_focus": True,
        "policy_focus": True
    }
)

TUOITRE_CONFIG = SourceConfig(
    name="tuoitre",
    base_url=os.getenv("TUOITRE_BASE_URL", "https://tuoitre.vn/rss"),
    base_domain=os.getenv("TUOITRE_BASE_DOMAIN", "https://tuoitre.vn"),
    rss_url=os.getenv("TUOITRE_RSS_URL", "https://tuoitre.vn/rss/tai-chinh-kinh-doanh.rss"),
    output_dir=os.getenv("TUOITRE_OUTPUT_DIR", "data/tuoitre"),
    source_type="rss",
    custom_settings={
        "vietnamese_content": True,
        "general_business_focus": True,
        "filter_by_category": ["tài chính", "kinh doanh"]
    }
)

VNEXPRESS_CONFIG = SourceConfig(
    name="vnexpress",
    base_url=os.getenv("VNEXPRESS_BASE_URL", "https://vnexpress.net/rss"),
    base_domain=os.getenv("VNEXPRESS_BASE_DOMAIN", "https://vnexpress.net"),
    rss_url=os.getenv("VNEXPRESS_RSS_URL", "https://vnexpress.net/rss/kinh-doanh.rss"),
    output_dir=os.getenv("VNEXPRESS_OUTPUT_DIR", "data/vnexpress"),
    source_type="rss",
    custom_settings={
        "vietnamese_content": True,
        "general_business_focus": True,
        "fast_news_cycle": True
    }
)

# Predefined LLM configurations
# ⚠️ API keys MUST be set in .env file, not hardcoded here!

OPENROUTER_CLAUDE_CONFIG = LLMConfig(
    provider="openrouter",
    api_key=os.getenv("OPENROUTER_API_KEY", ""),
    base_url=os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
    model_name=os.getenv("LLM_MODEL_NAME", "anthropic/claude-3.5-sonnet"),
    temperature=float(os.getenv("LLM_TEMPERATURE", "0.65")),
    max_tokens=int(os.getenv("LLM_MAX_TOKENS", "4096"))
)

OPENAI_GPT4_CONFIG = LLMConfig(
    provider="openai",
    api_key=os.getenv("OPENAI_API_KEY", ""),
    base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
    model_name=os.getenv("OPENAI_MODEL_NAME", "gpt-4"),
    temperature=float(os.getenv("LLM_TEMPERATURE", "0.65")),
    max_tokens=int(os.getenv("LLM_MAX_TOKENS", "4096"))
)

CUSTOM_LLM_CONFIG = LLMConfig(
    provider=os.getenv("CUSTOM_LLM_PROVIDER", "custom"),
    api_key=os.getenv("CUSTOM_LLM_API_KEY", ""),
    base_url=os.getenv("CUSTOM_LLM_BASE_URL", "https://api2.key4u.shop/v1"),
    model_name=os.getenv("CUSTOM_LLM_MODEL_NAME", "gpt-4.1-nano-2025-04-14"),
    temperature=float(os.getenv("LLM_TEMPERATURE", "0.65")),
    max_tokens=int(os.getenv("LLM_MAX_TOKENS", "4096"))
)


def create_default_config() -> SystemConfig:
    """Create default configuration for development"""
    sources = [VIETSTOCK_CONFIG]
    
    crawler_config = CrawlerConfig(
        sources=sources,
        storage=StorageConfig(
            storage_type="hybrid",
            mongodb_uri=os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
            database_name=os.getenv("DATABASE_NAME", "financial_news")
        ),
        interval_minutes=int(os.getenv("CRAWLER_INTERVAL_MINUTES", "60"))
    )
    
    extractor_config = ExtractorConfig(
        llm=CUSTOM_LLM_CONFIG,
        master_json=MasterJSONConfig(
            base_dir="data/master"
        )
    )
    
    api_config = APIConfig(
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", "8002")),
        reload=os.getenv("API_RELOAD", "true").lower() == "true"
    )
    
    return SystemConfig(
        crawler=crawler_config,
        extractor=extractor_config,
        api=api_config,
        environment="development",
        enabled_sources=["vietstock"]
    )


def create_multi_source_config() -> SystemConfig:
    """Create configuration with multiple sources"""
    sources = [VIETSTOCK_CONFIG, CAFEF_CONFIG, VIETNAMFINANCE_CONFIG, TUOITRE_CONFIG]
    
    crawler_config = CrawlerConfig(
        sources=sources,
        storage=StorageConfig(
            storage_type="hybrid",
            mongodb_uri=os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
            database_name=os.getenv("DATABASE_NAME", "financial_news")
        ),
        interval_minutes=int(os.getenv("CRAWLER_INTERVAL_MINUTES", "30"))
    )
    
    extractor_config = ExtractorConfig(
        llm=CUSTOM_LLM_CONFIG,
        master_json=MasterJSONConfig(
            base_dir="data/master"
        )
    )
    
    api_config = APIConfig(
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", "8002")),
        reload=os.getenv("API_RELOAD", "false").lower() == "true"
    )
    
    return SystemConfig(
        crawler=crawler_config,
        extractor=extractor_config,
        api=api_config,
        environment="production",
        enabled_sources=["vietstock", "cafef", "vietnamfinance", "tuoitre"],
        default_source="vietstock"
    )


def create_config_from_env() -> SystemConfig:
    """Create configuration from environment variables"""
    # Override based on environment
    multi_source = os.getenv("ENABLE_MULTI_SOURCE", "false").lower() == "true"
    
    if multi_source:
        return create_multi_source_config()
    else:
        return create_default_config()


def get_source_config_by_name(source_name: str) -> SourceConfig:
    """Get predefined source configuration by name"""
    source_configs = {
        "vietstock": VIETSTOCK_CONFIG,
        "cafef": CAFEF_CONFIG,
        "vietnamfinance": VIETNAMFINANCE_CONFIG,
        "tuoitre": TUOITRE_CONFIG,
        "vnexpress": VNEXPRESS_CONFIG
    }
    
    return source_configs.get(source_name.lower(), VIETSTOCK_CONFIG)


def get_llm_config_by_name(llm_name: str) -> LLMConfig:
    """Get predefined LLM configuration by name"""
    llm_configs = {
        "openrouter-claude": OPENROUTER_CLAUDE_CONFIG,
        "openai-gpt4": OPENAI_GPT4_CONFIG,
        "custom": CUSTOM_LLM_CONFIG
    }
    
    return llm_configs.get(llm_name.lower(), CUSTOM_LLM_CONFIG)