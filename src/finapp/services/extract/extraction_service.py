"""
Extraction Service for Financial News Analysis

This service orchestrates the batch processing of articles through LLM extraction,
managing sessions, storage, and providing a high-level interface for the extraction workflow.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

from ...config import Config
from ...schema.extractor import (
    FinancialNewsExtraction,
    ExtractionBatchResult,
    ExtractionSession
)
from ...schema.request import LLMExtractorResponse
from .extrator_agent import LLMExtractorAgent
from ...database.llm_extraction import LLMExtractionRepository


logger = logging.getLogger(__name__)


class ExtractionService:
    """
    High-level service for managing financial news extraction operations.
    
    Provides batch processing, session management, and result storage capabilities
    for extracting structured information from news articles using LLM.
    """
    
    def __init__(self, 
                 output_dir: Optional[str] = None,
                 batch_size: Optional[int] = None,
                 delay_seconds: Optional[float] = None):
        """
        Initialize the Extraction Service
        
        Args:
            output_dir: Directory to save extracted data (defaults to Config.EXTRACTOR_OUTPUT_DIR)
            batch_size: Number of articles per batch (defaults to Config.EXTRACTOR_BATCH_SIZE)
            delay_seconds: Delay between article extractions (defaults to Config.EXTRACTOR_DELAY_SECONDS)
        """
        self.output_dir = Path(output_dir or Config.EXTRACTOR_OUTPUT_DIR)
        self.batch_size = batch_size or Config.EXTRACTOR_BATCH_SIZE
        self.delay_seconds = delay_seconds or Config.EXTRACTOR_DELAY_SECONDS
        
        # Extractor agent will be initialized lazily when needed
        self.extractor = None
        
        # Active sessions storage
        self.active_sessions: Dict[str, ExtractionSession] = {}
        
        # MongoDB repository for extraction results
        self.mongo_repository = None
        try:
            self.mongo_repository = LLMExtractionRepository()
            logger.info("MongoDB LLM Extraction repository initialized")
        except Exception as e:
            logger.warning(f"Could not initialize MongoDB repository: {e}")
            logger.info("Results will be saved to files only")
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"ExtractionService initialized")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"Delay between articles: {self.delay_seconds}s")
        logger.info("Extractor agent will be initialized lazily")

    def _get_extractor(self) -> LLMExtractorAgent:
        """Get or create extractor agent lazily"""
        if self.extractor is None:
            self.extractor = LLMExtractorAgent()
            logger.info(f"Extractor agent initialized: {self.extractor.model_name}")
        return self.extractor
    
    def create_session(self, 
                      session_name: Optional[str] = None,
                      total_articles: Optional[int] = None) -> str:
        """
        Create a new extraction session
        
        Args:
            session_name: Optional name for the session
            total_articles: Total number of articles to process (if known)
            
        Returns:
            Session ID for tracking
        """
        session_id = str(uuid.uuid4())
        
        # Calculate total batches if total_articles is known
        total_batches = 0
        if total_articles:
            total_batches = (total_articles + self.batch_size - 1) // self.batch_size
        
        session = ExtractionSession(
            session_id=session_id,
            status="created",
            total_batches=total_batches,
            total_articles=total_articles or 0
        )
        
        self.active_sessions[session_id] = session
        
        # Save session info
        self._save_session(session)
        
        logger.info(f"Created extraction session: {session_id}")
        if session_name:
            logger.info(f"Session name: {session_name}")
        if total_articles:
            logger.info(f"Total articles: {total_articles}")
            logger.info(f"Estimated batches: {total_batches}")

        return session_id
    
    def process_articles_from_json(self, 
                                  json_file_path: str,
                                  session_id: Optional[str] = None,
                                  session_name: Optional[str] = None) -> LLMExtractorResponse:
        """
        Process articles from a JSON file
        
        Args:
            json_file_path: Path to JSON file containing articles
            session_id: Existing session ID (creates new if not provided)
            session_name: Optional session name
            
        Returns:
            LLMExtractorResponse with processing results
        """
        try:
            # Load articles from JSON file
            articles = self._load_articles_from_json(json_file_path)
            
            if not articles:
                return LLMExtractorResponse(
                    success=False,
                    message="No articles found in JSON file",
                    data={"file_path": json_file_path}
                )
            
            # Create session if not provided
            if not session_id:
                session_id = self.create_session(session_name, len(articles))
            else:
                # Update existing session
                if session_id in self.active_sessions:
                    session = self.active_sessions[session_id]
                    session.total_articles = len(articles)
                    session.total_batches = (len(articles) + self.batch_size - 1) // self.batch_size

            logger.info(f"Processing {len(articles)} articles from {json_file_path}")

            # Process articles in batches
            batch_results = []
            total_processed = 0
            total_successful = 0
            total_failed = 0
            
            for i in range(0, len(articles), self.batch_size):
                batch = articles[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1

                logger.info(f"Processing batch {batch_num}/{(len(articles) + self.batch_size - 1) // self.batch_size}")

                try:
                    # Update session status
                    if session_id in self.active_sessions:
                        self.active_sessions[session_id].status = "processing"
                    
                    # Process batch
                    extractor = self._get_extractor()
                    batch_result = extractor.extract_batch(
                        articles=batch,
                        delay_seconds=self.delay_seconds
                    )
                    
                    batch_results.append(batch_result)
                    total_processed += batch_result.total_articles
                    total_successful += batch_result.successful_extractions
                    total_failed += batch_result.failed_extractions
                    
                    # Save batch results
                    self._save_batch_results(session_id, batch_num, batch_result)
                    
                    # Update session progress
                    if session_id in self.active_sessions:
                        session = self.active_sessions[session_id]
                        session.completed_batches += 1
                        session.processed_articles += batch_result.total_articles
                        session.successful_extractions += batch_result.successful_extractions
                        session.failed_extractions += batch_result.failed_extractions
                        self._save_session(session)
                    
                    logger.info(f"Batch {batch_num} completed: {batch_result.successful_extractions}/{batch_result.total_articles} successful")
                    
                except Exception as e:
                    error_msg = f"Batch {batch_num} failed: {str(e)}"
                    logger.error(f"{error_msg}")
                    
                    # Update session with error
                    if session_id in self.active_sessions:
                        session = self.active_sessions[session_id]
                        session.errors.append(error_msg)
                        session.failed_extractions += len(batch)
                        session.processed_articles += len(batch)
                        self._save_session(session)
                    
                    total_failed += len(batch)
            
            # Complete session
            if session_id in self.active_sessions:
                session = self.active_sessions[session_id]
                session.status = "completed"
                session.end_time = datetime.now(timezone.utc)
                self._save_session(session)
            
            # Calculate overall results
            overall_success_rate = (total_successful / total_processed * 100) if total_processed > 0 else 0
            
            # Generate summary
            summary = self._generate_processing_summary(
                session_id=session_id,
                source_file=json_file_path,
                total_articles=total_processed,
                successful_extractions=total_successful,
                failed_extractions=total_failed,
                success_rate=overall_success_rate,
                batch_count=len(batch_results)
            )
            
            # Save summary
            summary_file = self.output_dir / f"session_{session_id}_summary.json"
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False, default=str)

            logger.info(f"Processing completed for session {session_id}")
            logger.info(f"Overall success rate: {overall_success_rate:.1f}% ({total_successful}/{total_processed})")

            return LLMExtractorResponse(
                success=True,
                message=f"Successfully processed {total_processed} articles",
                data={
                    "session_id": session_id,
                    "total_articles": total_processed,
                    "successful_extractions": total_successful,
                    "failed_extractions": total_failed,
                    "success_rate": round(overall_success_rate, 2),
                    "batch_count": len(batch_results),
                    "output_files": [str(summary_file)],
                    "summary": summary
                },
                session_id=session_id
            )
            
        except Exception as e:
            error_msg = f"Processing failed: {str(e)}"
            logger.error(f"{error_msg}")

            # Update session with error
            if session_id and session_id in self.active_sessions:
                session = self.active_sessions[session_id]
                session.status = "failed"
                session.end_time = datetime.now(timezone.utc)
                session.errors.append(error_msg)
                self._save_session(session)
            
            return LLMExtractorResponse(
                success=False,
                message=error_msg,
                data={"file_path": json_file_path},
                session_id=session_id
            )
    
    def get_session_status(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status of an extraction session
        
        Args:
            session_id: Session ID to check
            
        Returns:
            Session status dictionary or None if not found
        """
        if session_id in self.active_sessions:
            session = self.active_sessions[session_id]
            return {
                "session_id": session.session_id,
                "status": session.status,
                "progress_percentage": session.progress_percentage,
                "success_rate": session.success_rate,
                "total_batches": session.total_batches,
                "completed_batches": session.completed_batches,
                "total_articles": session.total_articles,
                "processed_articles": session.processed_articles,
                "successful_extractions": session.successful_extractions,
                "failed_extractions": session.failed_extractions,
                "start_time": session.start_time.isoformat(),
                "end_time": session.end_time.isoformat() if session.end_time else None,
                "errors": session.errors
            }
        
        # Try to load from file
        session_file = self.output_dir / f"session_{session_id}.json"
        if session_file.exists():
            try:
                with open(session_file, 'r', encoding='utf-8') as f:
                    session_data = json.load(f)
                return session_data
            except Exception as e:
                logger.error(f"Failed to load session {session_id}: {e}")

        return None
    
    def get_extraction_results(self, session_id: str) -> Dict[str, Any]:
        """
        Get all extraction results for a session
        
        Args:
            session_id: Session ID
            
        Returns:
            Dictionary containing all extraction results
        """
        results = {
            "session_id": session_id,
            "extractions": [],
            "batch_results": [],
            "summary": None
        }
        
        # Load batch results
        batch_pattern = f"session_{session_id}_batch_*.json"
        for batch_file in self.output_dir.glob(batch_pattern):
            try:
                with open(batch_file, 'r', encoding='utf-8') as f:
                    batch_data = json.load(f)
                results["batch_results"].append(batch_data)
                
                # Extract individual extractions
                if "results" in batch_data:
                    results["extractions"].extend(batch_data["results"])
                    
            except Exception as e:
                logger.error(f"Failed to load batch file {batch_file}: {e}")

        # Load summary if available
        summary_file = self.output_dir / f"session_{session_id}_summary.json"
        if summary_file.exists():
            try:
                with open(summary_file, 'r', encoding='utf-8') as f:
                    summary_data = json.load(f)
                results["summary"] = summary_data
            except Exception as e:
                logger.error(f"Failed to load summary file {summary_file}: {e}")

        return results
    
    def _load_articles_from_json(self, json_file_path: str) -> List[Dict[str, Any]]:
        """Load articles from JSON file"""
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle different JSON structures
            articles = []
            
            if "articles" in data:
                # Vietstock format
                for article in data["articles"]:
                    # Only process articles with HTML content
                    if article.get("html_extraction_success") and article.get("main_content"):
                        articles.append({
                            "title": article.get("title", ""),
                            "category": article.get("category", ""),
                            "description_text": article.get("description_text", ""),
                            "main_content": article.get("main_content", ""),
                            "guid": article.get("guid", ""),
                            "link": article.get("link", ""),
                            "pub_date": article.get("pub_date", "")
                        })
            elif isinstance(data, list):
                # Direct array of articles
                for article in data:
                    if article.get("html_extraction_success") and article.get("main_content"):
                        articles.append({
                            "title": article.get("title", ""),
                            "category": article.get("category", ""),
                            "description_text": article.get("description_text", ""),
                            "main_content": article.get("main_content", ""),
                            "guid": article.get("guid", ""),
                            "link": article.get("link", ""),
                            "pub_date": article.get("pub_date", "")
                        })

            logger.info(f"Loaded {len(articles)} articles with HTML content from {json_file_path}")
            return articles
            
        except Exception as e:
            logger.error(f"Failed to load articles from {json_file_path}: {e}")
            raise
    
    def _save_session(self, session: ExtractionSession) -> None:
        """Save session to file"""
        session_file = self.output_dir / f"session_{session.session_id}.json"
        with open(session_file, 'w', encoding='utf-8') as f:
            json.dump(session.dict(), f, indent=2, ensure_ascii=False, default=str)
    
    def _save_batch_results(self, session_id: str, batch_num: int, batch_result: ExtractionBatchResult) -> None:
        """Save batch results to file and MongoDB"""
        # Save to file (existing behavior)
        batch_file = self.output_dir / f"session_{session_id}_batch_{batch_num}.json"
        with open(batch_file, 'w', encoding='utf-8') as f:
            json.dump(batch_result.dict(), f, indent=2, ensure_ascii=False, default=str)
        
        # Save to MongoDB if available
        if self.mongo_repository and batch_result.results:
            mongo_success = 0
            mongo_failed = 0

            logger.info(f"Saving {len(batch_result.results)} extraction results to MongoDB...")

            for result in batch_result.results:
                try:
                    # Prepare extraction data for MongoDB
                    extraction_data = {
                        "sentiment_analysis": {
                            "overall_sentiment": result.sentiment_analysis.overall_sentiment,
                            "sentiment_score": result.sentiment_analysis.sentiment_score,
                            "key_factors": result.sentiment_analysis.key_factors,
                            "extraction_timestamp": result.extraction_timestamp,
                            "extraction_model": result.extraction_model,
                            "confidence": result.extraction_confidence
                        },
                        "stock_level": [
                            {
                                "ticker": stock.ticker,
                                "company_name": stock.company_name,
                                "sentiment": stock.sentiment,
                                "impact_type": stock.impact_type,
                                "price_impact": stock.price_impact,
                                "confidence": stock.confidence,
                                "extraction_timestamp": result.extraction_timestamp,
                                "extraction_model": result.extraction_model
                            }
                            for stock in result.stock_level
                        ],
                        "sector_level": [
                            {
                                "sector_name": sector.sector_name,
                                "sentiment": sector.sentiment,
                                "impact_description": sector.impact_description,
                                "affected_companies": sector.affected_companies,
                                "extraction_timestamp": result.extraction_timestamp,
                                "extraction_model": result.extraction_model
                            }
                            for sector in result.sector_level
                        ],
                        "market_level": {
                            "scope": result.market_level.scope,
                            "exchange": result.market_level.exchange,
                            "market_moving": result.market_level.market_moving,
                            "impact_magnitude": result.market_level.impact_magnitude,
                            "key_indices": result.market_level.key_indices,
                            "extraction_timestamp": result.extraction_timestamp,
                            "extraction_model": result.extraction_model
                        },
                        "financial_data": {
                            "has_numbers": result.financial_data.has_numbers,
                            "revenues": result.financial_data.revenues,
                            "profits": result.financial_data.profits,
                            "percentages": result.financial_data.percentages,
                            "amounts": result.financial_data.amounts,
                            "extraction_timestamp": result.extraction_timestamp,
                            "extraction_model": result.extraction_model
                        }
                    }
                    
                    # Save to MongoDB
                    if self.mongo_repository.save_complete_extraction(result.article_guid, extraction_data):
                        mongo_success += 1
                    else:
                        mongo_failed += 1
                        
                except Exception as e:
                    logger.error(f"Failed to save extraction for {result.article_guid}: {e}")
                    mongo_failed += 1

            logger.info(f"MongoDB batch save completed: {mongo_success} successful, {mongo_failed} failed")
        else:
            if not self.mongo_repository:
                logger.debug("MongoDB not available, saving to files only")
            else:
                logger.debug("No extraction results to save to MongoDB")

    def _generate_processing_summary(self,
                                    session_id: str,
                                    source_file: str,
                                    total_articles: int,
                                    successful_extractions: int,
                                    failed_extractions: int,
                                    success_rate: float,
                                    batch_count: int) -> Dict[str, Any]:
        """Generate processing summary"""
        return {
            "session_id": session_id,
            "source_file": source_file,
            "processing_summary": {
                "total_articles": total_articles,
                "successful_extractions": successful_extractions,
                "failed_extractions": failed_extractions,
                "success_rate": round(success_rate, 2),
                "batch_count": batch_count,
                "processing_date": datetime.now(timezone.utc).isoformat()
            },
            "configuration": {
                "batch_size": self.batch_size,
                "delay_seconds": self.delay_seconds,
                "model_used": self._get_extractor().model_name
            },
            "output_directory": str(self.output_dir)
        }
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the extractor model"""
        return self._get_extractor().get_model_info()
    
    def list_sessions(self) -> List[Dict[str, Any]]:
        """List all extraction sessions"""
        sessions = []
        
        # Active sessions
        for session_id, session in self.active_sessions.items():
            sessions.append({
                "session_id": session_id,
                "status": session.status,
                "total_articles": session.total_articles,
                "progress_percentage": session.progress_percentage,
                "start_time": session.start_time.isoformat(),
                "source": "active"
            })
        
        # Completed sessions from files
        for session_file in self.output_dir.glob("session_*.json"):
            try:
                with open(session_file, 'r', encoding='utf-8') as f:
                    session_data = json.load(f)
                
                session_id = session_data.get("session_id")
                if session_id and session_id not in self.active_sessions:
                    sessions.append({
                        "session_id": session_id,
                        "status": session_data.get("status", "unknown"),
                        "total_articles": session_data.get("total_articles", 0),
                        "progress_percentage": session_data.get("progress_percentage", 0),
                        "start_time": session_data.get("start_time"),
                        "source": "file"
                    })
            except Exception as e:
                logger.warning(f"Failed to load session file {session_file}: {e}")

        # Sort by start time (newest first)
        sessions.sort(key=lambda x: x.get("start_time", ""), reverse=True)
        return sessions