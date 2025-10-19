"""
Main Entry Point for Vietstock Crawler API

This module starts the FastAPI application with Vietstock crawler service.
"""

import logging
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from src.finapp.api.routes import crawler_router, extractor_router, master_json_router, system_config_router, llm_results_router
from src.finapp.services.crawl import VietstockCrawlerService, CrawlerScheduler
from src.finapp.config import Config

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
crawler_service: VietstockCrawlerService | None = None
scheduler: CrawlerScheduler | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global crawler_service, scheduler
    
    # Startup
    logger.info("🚀 Starting Vietstock Crawler API")
    
    try:
        # Initialize crawler service with config
        crawler_service = VietstockCrawlerService(
            base_dir="data",  # Unified data directory
            source_name="vietstock",  # Unified source name
        )
        logger.info(f"Crawler service initialized - storing in {crawler_service.storage.output_dir}")
        
        # Initialize scheduler (but don't start it automatically)
        scheduler = CrawlerScheduler(crawler_service, interval_minutes=Config.CRAWLER_INTERVAL_MINUTES)
        logger.info(f"Scheduler initialized with {Config.CRAWLER_INTERVAL_MINUTES} minutes interval (not started)")
        
        # Store in app state for access
        app.state.crawler = crawler_service
        app.state.scheduler = scheduler
        
        logger.info("Application startup completed")
        
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down application")
    
    if scheduler and scheduler.is_running:
        scheduler.stop()
        logger.info("Scheduler stopped")

    logger.info("Application shutdown completed")


# Create FastAPI app
app = FastAPI(
    title="Vietstock Crawler API",
    description="API for crawling financial news from Vietstock.vn",
    version="2.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(crawler_router)
app.include_router(extractor_router)
app.include_router(master_json_router)
app.include_router(system_config_router)
app.include_router(llm_results_router)

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Vietstock Crawler API",
        "version": "2.0.0",
        "crawler_initialized": crawler_service is not None,
        "scheduler_initialized": scheduler is not None,
        "scheduler_running": scheduler.is_running if scheduler else False
    }


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Vietstock Crawler & LLM Extractor API v2.0.0",
        "docs": "/docs",
        "health": "/health",
        "crawler_endpoints": {
            "start": "/crawl/start",
            "stop": "/crawl/stop", 
            "trigger": "/crawl/trigger",
            "stats": "/crawl/stats",
            "scheduler_status": "/crawl/scheduler/status",
            "config": "/crawl/config",
            "extract_html": "/crawl/extract-html",
            "restore_from_mongodb": "/crawl/restore-from-mongodb"
        },
        "extractor_endpoints": {
            "single_article": "/extract/single",
            "batch_articles": "/extract/batch",
            "process_file": "/extract/process-file",
            "upload_and_process": "/extract/upload-and-process",
            "sessions": "/extract/sessions",
            "model_info": "/extract/model/info",
            "config": "/extract/config"
        },
        "master_json_endpoints": {
            "process_daily": "/master/process-daily",
            "daily_summary": "/master/summary/{date}",
            "query_data": "/master/query/{date}",
            "stock_analysis": "/master/stock/{date}/{ticker}",
            "sector_analysis": "/master/sector/{date}/{sector}",
            "search": "/master/search/{date}",
            "market_insights": "/master/insights/{date}",
            "export_report": "/master/export-report/{date}",
            "available_dates": "/master/available-dates"
        },
        "system_config_endpoints": {
            "system_info": "/system/info",
            "override_config": "/system/config/override",
            "recreate_services": "/system/services/recreate",
            "switch_sources": "/system/sources/switch",
            "available_scenarios": "/system/scenarios",
            "switch_llm": "/system/llm/switch",
            "services_status": "/system/services/status",
            "export_config": "/system/config/export"
        }
    }


def main():
    """Main entry point"""
    port = Config.API_PORT
    host = Config.API_HOST
    reload = Config.API_RELOAD

    logger.info(f"Starting server on {host}:{port}")

    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        log_level=getattr(logging, Config.LOG_LEVEL),
        reload=reload
    )

if __name__ == "__main__":
    main()
