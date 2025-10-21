"""
API Router for Financial News LLM Extractor

This module provides REST API endpoints for extracting structured financial information
from news articles using Large Language Models.
"""

import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query, UploadFile, File
from pydantic import BaseModel, Field

from ...services.extract.extraction_service import ExtractionService
from ...services.extract.extrator_agent import LLMExtractorAgent
from ...schema.request import LLMExtractorResponse
from ...config import Config
from .auto_processor import AutoProcessor
from ...services.crawl.storage import StorageService
from ...config.dataclasses import StorageConfig, SourceConfig

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/extract", tags=["LLM Extractor"])

# Global instances (in production, these would be managed with proper DI)
_extraction_service: Optional[ExtractionService] = None
_extractor_agent: Optional[LLMExtractorAgent] = None


def get_extraction_service() -> ExtractionService:
    """Get or create extraction service instance"""
    global _extraction_service
    if _extraction_service is None:
        _extraction_service = ExtractionService()
    return _extraction_service


def get_extractor_agent() -> LLMExtractorAgent:
    """Get or create extractor agent instance"""
    global _extractor_agent
    if _extractor_agent is None:
        _extractor_agent = LLMExtractorAgent()
    return _extractor_agent


# Request Models
class ExtractionRequest(BaseModel):
    """Request model for article extraction"""
    title: str = Field(..., description="Article title")
    category: str = Field(..., description="Article category")
    description_text: str = Field(..., description="Article description/summary")
    main_content: str = Field(..., description="Full article content")
    article_guid: str = Field(..., description="Unique article identifier")


class BatchExtractionRequest(BaseModel):
    """Request model for batch extraction"""
    articles: list[ExtractionRequest] = Field(..., description="List of articles to extract")
    session_name: Optional[str] = Field(None, description="Optional session name for tracking")
    delay_seconds: Optional[float] = Field(None, ge=0, description="Delay between extractions")


class FileProcessingRequest(BaseModel):
    """Request model for processing JSON file"""
    session_name: Optional[str] = Field(None, description="Optional session name for tracking")
    delay_seconds: Optional[float] = Field(None, ge=0, description="Delay between extractions")


# Response Models
class ModelInfoResponse(BaseModel):
    """Response model for model information"""
    model_name: str
    temperature: float
    max_tokens: int
    base_url: str
    api_configured: bool


class SessionListResponse(BaseModel):
    """Response model for session list"""
    sessions: list[Dict[str, Any]]
    total_count: int


# Extraction endpoints
@router.post("/single", response_model=LLMExtractorResponse)
async def extract_single_article(request: ExtractionRequest):
    """
    Extract structured information from a single article
    
    Args:
        request: Article data to extract
        
    Returns:
        LLMExtractorResponse with extraction results
    """
    try:
        extractor = get_extractor_agent()
        
        logger.info(f"Extracting data for article: {request.article_guid}")
        
        # Extract article data
        result = extractor.extract_single_article(
            title=request.title,
            category=request.category,
            description_text=request.description_text,
            main_content=request.main_content,
            article_guid=request.article_guid
        )
        
        logger.info(f"Extraction completed for {request.article_guid}")
        
        return LLMExtractorResponse(
            success=True,
            message="Article extraction completed successfully",
            data={
                "extraction": result.dict(),
                "confidence": result.extraction_confidence,
                "model_used": result.extraction_model
            }
        )
        
    except Exception as e:
        logger.error(f"Single article extraction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Extraction failed: {e}")


@router.post("/batch", response_model=LLMExtractorResponse)
async def extract_batch_articles(request: BatchExtractionRequest):
    """
    Extract structured information from multiple articles
    
    Args:
        request: Batch extraction request
        
    Returns:
        LLMExtractorResponse with batch extraction results
    """
    try:
        service = get_extraction_service()
        
        # Create session
        session_id = service.create_session(
            session_name=request.session_name,
            total_articles=len(request.articles)
        )
        
        logger.info(f"Starting batch extraction for {len(request.articles)} articles")
        
        # Convert articles to dict format
        articles = [
            {
                "title": article.title,
                "category": article.category,
                "description_text": article.description_text,
                "main_content": article.main_content,
                "guid": article.article_guid
            }
            for article in request.articles
        ]
        
        # Process batch
        batch_result = service.extractor.extract_batch(
            articles=articles,
            delay_seconds=request.delay_seconds or Config.EXTRACTOR_DELAY_SECONDS
        )
        
        # Save results
        service._save_batch_results(session_id, 1, batch_result)
        
        # Update session
        if session_id in service.active_sessions:
            session = service.active_sessions[session_id]
            session.status = "completed"
            session.end_time = datetime.now(timezone.utc)
            session.processed_articles = batch_result.total_articles
            session.successful_extractions = batch_result.successful_extractions
            session.failed_extractions = batch_result.failed_extractions
            session.completed_batches = 1
            service._save_session(session)
        
        logger.info(f"Batch extraction completed: {batch_result.successful_extractions}/{batch_result.total_articles} successful")
        
        return LLMExtractorResponse(
            success=True,
            message=f"Batch extraction completed successfully",
            data={
                "session_id": session_id,
                "total_articles": batch_result.total_articles,
                "successful_extractions": batch_result.successful_extractions,
                "failed_extractions": batch_result.failed_extractions,
                "success_rate": round(batch_result.success_rate, 2),
                "extraction_time_seconds": batch_result.extraction_time_seconds,
                "results": [result.dict() for result in batch_result.results]
            },
            session_id=session_id
        )
        
    except Exception as e:
        logger.error(f"Batch extraction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Batch extraction failed: {e}")


@router.post("/process-file", response_model=LLMExtractorResponse)
async def process_json_file(
    file_path: str = Query(..., description="Path to JSON file containing articles"),
    session_name: Optional[str] = Query(None, description="Optional session name for tracking"),
    delay_seconds: Optional[float] = Query(None, ge=0, description="Delay between extractions")
):
    """
    Process articles from a JSON file
    
    Args:
        file_path: Path to JSON file
        session_name: Optional session name
        delay_seconds: Delay between extractions
        
    Returns:
        LLMExtractorResponse with processing results
    """
    try:
        service = get_extraction_service()
        
        # Validate file path
        json_file = Path(file_path)
        if not json_file.exists():
            raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
        
        if not json_file.suffix.lower() == '.json':
            raise HTTPException(status_code=400, detail="File must be a JSON file")
        
        logger.info(f"Processing articles from file: {file_path}")
        
        # Process file
        result = service.process_articles_from_json(
            json_file_path=str(json_file),
            session_name=session_name
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"File processing failed: {e}")
        raise HTTPException(status_code=500, detail=f"File processing failed: {e}")


@router.post("/upload-and-process", response_model=LLMExtractorResponse)
async def upload_and_process_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="JSON file containing articles"),
    session_name: Optional[str] = Query(None, description="Optional session name for tracking"),
    delay_seconds: Optional[float] = Query(None, ge=0, description="Delay between extractions")
):
    """
    Upload and process a JSON file containing articles
    
    Args:
        file: Uploaded JSON file
        session_name: Optional session name
        delay_seconds: Delay between extractions
        
    Returns:
        LLMExtractorResponse with processing results
    """
    try:
        # Validate file type
        if not file.filename.endswith('.json'):
            raise HTTPException(status_code=400, detail="File must be a JSON file")
        
        # Create upload directory
        upload_dir = Path("data/uploads")
        upload_dir.mkdir(parents=True, exist_ok=True)
        
        # Save uploaded file
        file_path = upload_dir / file.filename
        
        with open(file_path, 'wb') as buffer:
            content = await file.read()
            buffer.write(content)
        
        logger.info(f"File uploaded: {file_path}")
        
        # Process file in background
        service = get_extraction_service()
        
        def process_background():
            try:
                result = service.process_articles_from_json(
                    json_file_path=str(file_path),
                    session_name=session_name or f"upload_{file.filename}"
                )
                logger.info(f"Background processing completed for {file.filename}")
            except Exception as e:
                logger.error(f"Background processing failed for {file.filename}: {e}")
        
        background_tasks.add_task(process_background)
        
        return LLMExtractorResponse(
            success=True,
            message="File uploaded successfully. Processing started in background.",
            data={
                "file_path": str(file_path),
                "file_size": len(content),
                "status": "processing_started"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"File upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"File upload failed: {e}")


# Session management endpoints
@router.get("/sessions", response_model=SessionListResponse)
async def list_extraction_sessions():
    """
    List all extraction sessions
    
    Returns:
        SessionListResponse with session information
    """
    try:
        service = get_extraction_service()
        sessions = service.list_sessions()
        
        return SessionListResponse(
            sessions=sessions,
            total_count=len(sessions)
        )
        
    except Exception as e:
        logger.error(f"Failed to list sessions: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list sessions: {e}")


@router.get("/sessions/{session_id}/status")
async def get_session_status(session_id: str):
    """
    Get status of a specific extraction session
    
    Args:
        session_id: Session ID to check
        
    Returns:
        Session status information
    """
    try:
        service = get_extraction_service()
        status = service.get_session_status(session_id)
        
        if status is None:
            raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")
        
        return {
            "success": True,
            "message": "Session status retrieved successfully",
            "data": status
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get session status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get session status: {e}")


@router.get("/sessions/{session_id}/results")
async def get_extraction_results(session_id: str):
    """
    Get all extraction results for a session
    
    Args:
        session_id: Session ID
        
    Returns:
        All extraction results for the session
    """
    try:
        service = get_extraction_service()
        results = service.get_extraction_results(session_id)
        
        if not results["extractions"] and not results["batch_results"]:
            raise HTTPException(status_code=404, detail=f"No results found for session: {session_id}")
        
        return {
            "success": True,
            "message": "Extraction results retrieved successfully",
            "data": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get extraction results: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get extraction results: {e}")


# Configuration and info endpoints
@router.get("/model/info", response_model=ModelInfoResponse)
async def get_model_info():
    """
    Get information about the LLM model configuration
    
    Returns:
        Model configuration information
    """
    try:
        extractor = get_extractor_agent()
        model_info = extractor.get_model_info()
        
        return ModelInfoResponse(**model_info)
        
    except Exception as e:
        logger.error(f"Failed to get model info: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get model info: {e}")


@router.post("/auto-process-date", response_model=LLMExtractorResponse)
async def auto_process_date(
    background_tasks: BackgroundTasks,
    target_date: Optional[str] = Query(None, description="Target date in YYYYMMDD format (default: today)"),
    session_name: Optional[str] = Query(None, description="Optional session name for tracking"),
    delay_seconds: Optional[float] = Query(None, ge=0, description="Delay between extractions"),
    auto_mode: bool = Query(True, description="Whether to use auto mode with enhanced logging")
):
    """
    Automatically process articles for a specific date
    
    Args:
        target_date: Target date in YYYYMMDD format (default: today)
        session_name: Optional session name
        delay_seconds: Delay between extractions
        auto_mode: Enable auto mode with enhanced logging and error recovery
        
    Returns:
        LLMExtractorResponse with processing results
    """
    try:
        
        # Determine target date
        if target_date:
            try:
                target_date_obj = datetime.strptime(target_date, "%Y%m%d").date()
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use YYYYMMDD format.")
        else:
            target_date_obj = datetime.now().date()
            target_date = target_date_obj.strftime("%Y%m%d")
        
        # Find JSON file for the date
        config = Config()
        vietstock_config = config.CRAWLER_SOURCE_CONFIGS.get('vietstock', {})
        vietstock_output_dir = vietstock_config.get('output_dir', 'data/vietstock')
        json_file = Path(f"{vietstock_output_dir}/{target_date}/articles_{target_date}.json")
        
        if not json_file.exists():
            # Try to restore from MongoDB if file doesn't exist
            
            storage_config = StorageConfig(
                storage_type="mongodb",
                base_dir=config.CRAWLER_OUTPUT_DIR,
                database_name=config.DATABASE_NAME
            )
            source_config = SourceConfig(
                name="vietstock",
                base_url=vietstock_config.get('base_url', 'https://vietstock.vn/rss'),
                base_domain=vietstock_config.get('base_domain', 'https://vietstock.vn'),
                output_dir=vietstock_output_dir
            )
            storage_service = StorageService(storage_config, source_config)
            restored = storage_service.restore_from_mongodb(target_date)
            
            if restored:
                json_file = Path(f"{vietstock_output_dir}/{target_date}/articles_{target_date}.json")
            else:
                raise HTTPException(status_code=404, detail=f"No articles found for date {target_date}")
        
        logger.info(f"Auto-processing articles for date: {target_date}")
        logger.info(f"JSON file: {json_file}")
        
        if auto_mode:
            # Run in background with enhanced logging
            service = get_extraction_service()
            session_id = service.create_session(
                session_name or f"auto-{target_date}",
                None  # Will be determined after loading file
            )
            
            def process_with_logging():
                try:
                    processor = AutoProcessor()
                    result = processor.process_date_with_logging(
                        target_date=target_date,
                        session_id=session_id,
                        delay_seconds=delay_seconds or Config.EXTRACTOR_DELAY_SECONDS
                    )
                    # result is a dict, not an object with attributes
                    success_status = result.get('success', False) if isinstance(result, dict) else result.success
                    logger.info(f"Auto-processing completed for {target_date}: success={success_status}")
                except Exception as e:
                    logger.error(f"Auto-processing failed for {target_date}: {e}")
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    # Update session with error
                    if session_id in service.active_sessions:
                        service.active_sessions[session_id].status = "failed"
                        service.active_sessions[session_id].errors.append(str(e))
                        service._save_session(service.active_sessions[session_id])
            
            background_tasks.add_task(process_with_logging)
            
            return LLMExtractorResponse(
                success=True,
                message=f"Auto-processing started for date {target_date}",
                data={
                    "session_id": session_id,
                    "target_date": target_date,
                    "json_file": str(json_file),
                    "auto_mode": True,
                    "status": "processing_started"
                },
                session_id=session_id
            )
        else:
            # Run immediately
            service = get_extraction_service()
            result = service.process_articles_from_json(
                json_file_path=str(json_file),
                session_name=session_name
            )
            return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Auto-processing failed: {e}")
        raise HTTPException(status_code=500, detail=f"Auto-processing failed: {e}")


@router.get("/config")
async def get_extractor_config():
    """
    Get extractor configuration
    
    Returns:
        Extractor configuration information
    """
    try:
        service = get_extraction_service()
        extractor = get_extractor_agent()
        
        return {
            "success": True,
            "message": "Extractor configuration retrieved successfully",
            "data": {
                "output_directory": str(service.output_dir),
                "batch_size": service.batch_size,
                "delay_seconds": service.delay_seconds,
                "model": extractor.get_model_info(),
                "openrouter_configured": bool(Config.OPENROUTER_API_KEY),
                "supported_models": [
                    "anthropic/claude-3.5-sonnet",
                    "anthropic/claude-3-sonnet",
                    "openai/gpt-4",
                    "openai/gpt-4-turbo",
                    "google/gemini-pro"
                ]
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get extractor config: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get extractor config: {e}")


@router.get("/logs/{session_id}")
async def get_extraction_logs(session_id: str):
    """
    Get detailed logs for an extraction session
    
    Args:
        session_id: Session ID
        
    Returns:
        Detailed logs and error information
    """
    try:
        service = get_extraction_service()
        
        # Get session status
        status = service.get_session_status(session_id)
        if not status:
            raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")
        
        # Get extraction results
        results = service.get_extraction_results(session_id)
        
        # Extract logs from batch results
        logs = []
        error_logs = []
        
        for batch_result in results.get("batch_results", []):
            if "errors" in batch_result:
                for error in batch_result["errors"]:
                    error_logs.append({
                        "timestamp": batch_result.get("batch_timestamp"),
                        "error": error,
                        "type": "extraction_error"
                    })
            
            if "results" in batch_result:
                for extraction in batch_result["results"]:
                    logs.append({
                        "article_guid": extraction.get("article_guid"),
                        "article_title": extraction.get("article_title", "")[:50] + "...",
                        "extraction_timestamp": extraction.get("extraction_timestamp"),
                        "model_used": extraction.get("extraction_model"),
                        "confidence": extraction.get("extraction_confidence"),
                        "status": "success"
                    })
        
        return {
            "success": True,
            "message": "Extraction logs retrieved successfully",
            "data": {
                "session_id": session_id,
                "session_status": status,
                "total_logs": len(logs),
                "total_errors": len(error_logs),
                "successful_extractions": len(logs),
                "logs": logs,
                "error_logs": error_logs,
                "summary": results.get("summary", {})
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get extraction logs: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get extraction logs: {e}")


@router.get("/available-dates")
async def get_available_dates():
    """
    Get list of available dates for processing
    
    Returns:
        List of available dates with article counts
    """
    try:

        storage_config = StorageConfig(
            storage_type="mongodb",
            base_dir="data",
            database_name="financial_news"
        )
        source_config = SourceConfig(
            name="vietstock",
            base_url="https://vietstock.vn",
            base_domain="vietstock.vn",
            output_dir="vietstock"
        )
        storage = StorageService(storage_config, source_config)
        available_dates = []
        
        # Check last 30 days
        base_dir = Path("data/vietstock")
        if base_dir.exists():
            for date_dir in base_dir.iterdir():
                if date_dir.is_dir() and date_dir.name.isdigit() and len(date_dir.name) == 8:
                    try:
                        date_obj = datetime.strptime(date_dir.name, "%Y%m%d").date()
                        
                        # Check if articles file exists
                        articles_file = date_dir / f"articles_{date_dir.name}.json"
                        if articles_file.exists():
                            with open(articles_file, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            
                            # Count articles with HTML content
                            html_articles = sum(1 for article in data.get("articles", []) 
                                             if article.get("html_extraction_success", False))
                            
                            available_dates.append({
                                "date": date_dir.name,
                                "formatted_date": date_obj.strftime("%Y-%m-%d"),
                                "total_articles": len(data.get("articles", [])),
                                "html_articles": html_articles,
                                "file_path": str(articles_file),
                                "file_size_mb": articles_file.stat().st_size / (1024 * 1024)
                            })
                    except (ValueError, json.JSONDecodeError, KeyError):
                        continue
            
        # Sort by date (newest first)
        available_dates.sort(key=lambda x: x["date"], reverse=True)
        
        return {
            "success": True,
            "message": "Available dates retrieved successfully",
            "data": {
                "total_dates": len(available_dates),
                "dates": available_dates
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get available dates: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get available dates: {e}")


__all__ = ["router"]