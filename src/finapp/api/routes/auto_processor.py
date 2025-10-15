"""
Auto Processor for Enhanced LLM Extraction

This module provides enhanced processing with detailed logging,
error handling, and recovery mechanisms for LLM extraction.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional

from ...services.extract.extraction_service import ExtractionService
from ...services.extract.extrator_agent import LLMExtractorAgent
from ...schema.extractor import FinancialNewsExtraction, ExtractionBatchResult


logger = logging.getLogger(__name__)


class AutoProcessor:
    """
    Enhanced processor for automated LLM extraction with comprehensive logging
    and error handling capabilities.
    """
    
    def __init__(self):
        """Initialize the AutoProcessor"""
        self.extraction_service = ExtractionService()
        self.extractor = None
        
        # Enhanced logging setup
        self.setup_enhanced_logging()
        
        logger.info("🤖 AutoProcessor initialized with enhanced logging")
    
    def setup_enhanced_logging(self):
        """Setup enhanced logging configuration"""
        # Create dedicated log directory
        log_dir = Path("data/extracted/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create file handler for detailed logs
        log_file = log_dir / f"auto_processor_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        auto_logger = logging.getLogger('auto_processor')
        auto_logger.addHandler(file_handler)
        auto_logger.addHandler(console_handler)
        auto_logger.setLevel(logging.DEBUG)
        
        self.logger = auto_logger
    
    def _initialize_extractor(self) -> LLMExtractorAgent:
        """Initialize extractor with enhanced error handling"""
        try:
            self.extractor = LLMExtractorAgent()
            self.logger.info(f"✅ LLM Extractor initialized: {self.extractor.model_name}")
            return self.extractor
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize LLM Extractor: {e}")
            raise
    
    def process_date_with_logging(self, 
                                 target_date: str,
                                 session_id: str,
                                 delay_seconds: float = 1.0) -> Dict[str, Any]:
        """
        Process articles for a specific date with comprehensive logging
        
        Args:
            target_date: Target date in YYYYMMDD format
            session_id: Session ID for tracking
            delay_seconds: Delay between extractions
            
        Returns:
            Processing result dictionary
        """
        start_time = time.time()
        
        self.logger.info("=" * 80)
        self.logger.info(f"🚀 Starting Auto-Processing for Date: {target_date}")
        self.logger.info(f"📅 Session ID: {session_id}")
        self.logger.info(f"⏱️ Delay between articles: {delay_seconds}s")
        self.logger.info("=" * 80)
        
        try:
            # Initialize extractor
            extractor = self._initialize_extractor()
            
            # Load articles from JSON file
            json_file = Path(f"data/vietstock/{target_date}/articles_{target_date}.json")
            
            self.logger.info(f"📁 Loading articles from: {json_file}")
            
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            articles = data.get('articles', [])
            self.logger.info(f"📊 Found {len(articles)} articles in JSON file")
            
            # Filter articles with HTML content
            html_articles = [
                article for article in articles 
                if article.get('html_extraction_success', False) and article.get('main_content')
            ]
            
            self.logger.info(f"🌐 Articles with HTML content: {len(html_articles)}")
            
            if not html_articles:
                warning_msg = f"No articles with HTML content found for date {target_date}"
                self.logger.warning(f"⚠️ {warning_msg}")
                return {
                    "success": False,
                    "message": warning_msg,
                    "target_date": target_date,
                    "session_id": session_id,
                    "total_articles": len(articles),
                    "html_articles": 0,
                    "processed_articles": 0,
                    "successful_extractions": 0,
                    "failed_extractions": 0,
                    "extraction_time_seconds": 0
                }
            
            # Convert to required format
            articles_to_process = []
            for article in html_articles:
                articles_to_process.append({
                    'title': article.get('title', ''),
                    'category': article.get('category', ''),
                    'description_text': article.get('description_text', ''),
                    'main_content': article.get('main_content', ''),
                    'guid': article.get('guid', ''),
                    'link': article.get('link', ''),
                    'pub_date': article.get('pub_date', '')
                })
            
            # Process articles in batches
            total_processed = 0
            total_successful = 0
            total_failed = 0
            batch_results = []
            error_details = []
            
            batch_size = self.extraction_service.batch_size
            total_batches = (len(articles_to_process) + batch_size - 1) // batch_size
            
            self.logger.info(f"📦 Processing {len(articles_to_process)} articles in {total_batches} batches of {batch_size}")
            
            for i in range(0, len(articles_to_process), batch_size):
                batch_num = i + 1
                batch = articles_to_process[i:i + batch_size]
                
                self.logger.info(f"📦 Processing Batch {batch_num}/{total_batches} ({len(batch)} articles)")
                
                try:
                    # Process batch with enhanced logging
                    batch_result = self._process_batch_with_logging(
                        batch=batch,
                        batch_num=batch_num,
                        total_batches=total_batches,
                        delay_seconds=delay_seconds
                    )
                    
                    batch_results.append(batch_result)
                    total_processed += batch_result.total_articles
                    total_successful += batch_result.successful_extractions
                    total_failed += batch_result.failed_extractions
                    
                    self.logger.info(f"✅ Batch {batch_num} completed: {batch_result.successful_extractions}/{batch_result.total_articles} successful")
                    
                    # Save batch results
                    self._save_batch_results(session_id, batch_num, batch_result, target_date)
                    
                    # Update session progress
                    if session_id in self.extraction_service.active_sessions:
                        session = self.extraction_service.active_sessions[session_id]
                        session.completed_batches += 1
                        session.processed_articles += batch_result.total_articles
                        session.successful_extractions += batch_result.successful_extractions
                        session.failed_extractions += batch_result.failed_extractions
                        self.extraction_service._save_session(session)
                    
                    # Log individual article results
                    if batch_result.errors:
                        for error in batch_result.errors:
                            self.logger.error(f"❌ Article Error: {error}")
                            error_details.append(error)
                    
                    if batch_result.results:
                        for result in batch_result.results:
                            self.logger.info(f"✅ Article Extracted: {result.article_title[:50]}...")
                            self.logger.debug(f"   - GUID: {result.article_guid}")
                            self.logger.debug(f"   - Confidence: {result.extraction_confidence}")
                            self.logger.debug(f"   - Model: {result.extraction_model}")
                            self.logger.debug(f"   - Stock Tickers: {len(result.stock_tickers)}")
                            self.logger.debug(f"   - Sectors: {len(result.sectors_industries)}")
                    
                except Exception as e:
                    error_msg = f"Batch {batch_num} processing failed: {str(e)}"
                    self.logger.error(f"❌ {error_msg}")
                    error_details.append(error_msg)
                    total_failed += len(batch)
                    
                    # Update session with batch error
                    if session_id in self.extraction_service.active_sessions:
                        session = self.extraction_service.active_sessions[session_id]
                        session.failed_extractions += len(batch)
                        session.processed_articles += len(batch)
                        session.errors.append(error_msg)
                        self.extraction_service._save_session(session)
                
                # Small delay between batches
                if i + batch_size < len(articles_to_process):
                    time.sleep(delay_seconds)
            
            # Calculate final statistics
            extraction_time = time.time() - start_time
            success_rate = (total_successful / total_processed * 100) if total_processed > 0 else 0
            
            # Update session to completed
            if session_id in self.extraction_service.active_sessions:
                session = self.extraction_service.active_sessions[session_id]
                session.status = "completed"
                session.end_time = datetime.now(timezone.utc)
                session.total_articles = len(articles_to_process)
                self.extraction_service._save_session(session)
            
            # Generate comprehensive summary
            summary = self._generate_comprehensive_summary(
                session_id=session_id,
                target_date=target_date,
                total_articles=len(articles_to_process),
                html_articles=len(html_articles),
                total_processed=total_processed,
                total_successful=total_successful,
                total_failed=total_failed,
                total_batches=len(batch_results),
                extraction_time=extraction_time,
                success_rate=success_rate,
                batch_results=batch_results,
                error_details=error_details
            )
            
            # Save summary
            self._save_comprehensive_summary(session_id, summary, target_date)
            
            self.logger.info("=" * 80)
            self.logger.info(f"🎉 Auto-Processing Completed for {target_date}")
            self.logger.info(f"📊 Final Statistics:")
            self.logger.info(f"   - Total Articles: {len(articles_to_process)}")
            self.logger.info(f"   - Articles with HTML: {len(html_articles)}")
            self.extraction_service.active_sessions[session_id].status = "completed"
            self.extraction_service._save_session(session)
            self.extraction_service.active_sessions[session_id].end_time = datetime.now(timezone.utc)
            self.extraction_service.active_sessions[session_id].total_articles = len(articles_to_process)
            self.logger.info(f"   - Processed: {total_processed}")
            self.logger.info(f"   - Successful: {total_successful}")
            self.logger.info(f"   - Failed: {total_failed}")
            self.logger.info(f"   - Success Rate: {success_rate:.1f}%")
            self.logger.info(f"   - Extraction Time: {extraction_time:.2f}s")
            self.logger.info(f"   - Average Time/Article: {extraction_time/total_processed:.2f}s")
            self.logger.info("=" * 80)
            
            return {
                "success": True,
                "message": f"Auto-processing completed successfully for {target_date}",
                "target_date": target_date,
                "session_id": session_id,
                "total_articles": len(articles_to_process),
                "html_articles": len(html_articles),
                "processed_articles": total_processed,
                "successful_extractions": total_successful,
                "failed_extractions": total_failed,
                "success_rate": round(success_rate, 2),
                "total_batches": len(batch_results),
                "extraction_time_seconds": round(extraction_time, 2),
                "average_time_per_article": round(extraction_time/total_processed, 2) if total_processed > 0 else 0,
                "error_details": error_details
            }
            
        except Exception as e:
            self.logger.error(f"❌ Auto-processing failed for {target_date}: {e}")
            self.logger.exception("Full traceback:")
            
            # Update session with fatal error
            if session_id in self.extraction_service.active_sessions:
                session = self.extraction_service.active_sessions[session_id]
                session.status = "failed"
                session.end_time = datetime.now(timezone.utc)
                session.errors.append(f"Fatal error: {str(e)}")
                self.extraction_service._save_session(session)
            
            return {
                "success": False,
                "message": f"Auto-processing failed for {target_date}: {str(e)}",
                "target_date": target_date,
                "session_id": session_id,
                "error": str(e),
                "traceback": str(e)
            }
    
    def _process_batch_with_logging(self, 
                                batch: List[Dict[str, Any]],
                                batch_num: int,
                                total_batches: int,
                                delay_seconds: float) -> ExtractionBatchResult:
        """Process a batch of articles with detailed logging"""
        batch_start_time = time.time()
        
        self.logger.debug(f"🔄 Starting batch {batch_num} processing")
        self.logger.debug(f"   - Articles in batch: {len(batch)}")
        self.logger.debug(f"   - Delay settings: {delay_seconds}s")
        
        try:
            # Use the extraction service's extractor (initialized lazily)
            extractor = self.extraction_service._get_extractor()
            
            # Process the batch
            batch_result = extractor.extract_batch(
                articles=batch,
                delay_seconds=delay_seconds
            )
            
            batch_time = time.time() - batch_start_time
            
            self.logger.debug(f"✅ Batch {batch_num} completed in {batch_time:.2f}s")
            self.logger.debug(f"   - Success: {batch_result.successful_extractions}")
            self.logger.debug(f"   - Failed: {batch_result.failed_extractions}")
            self.logger.debug(f"   - Success Rate: {batch_result.success_rate:.1f}%")
            
            return batch_result
            
        except Exception as e:
            self.logger.error(f"❌ Batch {batch_num} failed: {e}")
            self.logger.exception("Batch processing error details:")
            
            # Create failed batch result
            failed_result = ExtractionBatchResult(
                total_articles=len(batch),
                successful_extractions=0,
                failed_extractions=len(batch),
                extraction_time_seconds=time.time() - batch_start_time,
                results=[],
                errors=[f"Batch {batch_num} failed: {str(e)}"]
            )
            
            return failed_result
    
    def _save_batch_results(self, session_id: str, batch_num: int, batch_result: ExtractionBatchResult, target_date: str):
        """Save batch results with enhanced metadata"""
        try:
            output_dir = Path("data/extracted") / target_date / session_id
            output_dir.mkdir(parents=True, exist_ok=True)
            
            batch_file = output_dir / f"batch_{batch_num:03d}_results.json"
            
            # Enhanced batch data with metadata
            enhanced_batch_data = {
                **batch_result.dict(),
                "batch_metadata": {
                    "batch_number": batch_num,
                    "target_date": target_date,
                    "session_id": session_id,
                    "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                    "extraction_model": self.extractor.model_name if self.extractor else "unknown",
                    "model_temperature": self.extractor.temperature if self.extractor else 0.1,
                    "batch_size": len(batch_result.results) if batch_result.results else 0
                }
            }
            
            with open(batch_file, 'w', encoding='utf-8') as f:
                json.dump(enhanced_batch_data, f, indent=2, ensure_ascii=False, default=str)
            
            self.logger.debug(f"💾 Saved batch {batch_num} results to {batch_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save batch {batch_num} results: {e}")
    
    def _generate_comprehensive_summary(self, **kwargs) -> Dict[str, Any]:
        """Generate comprehensive processing summary"""
        return {
            "processing_info": {
                "target_date": kwargs.get("target_date"),
                "session_id": kwargs.get("session_id"),
                "processor": "AutoProcessor",
                "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                "processing_mode": "auto_with_logging"
            },
            "article_statistics": {
                "total_articles_available": kwargs.get("total_articles", 0),
                "articles_with_html": kwargs.get("html_articles", 0),
                "articles_processed": kwargs.get("processed_articles", 0),
                "successful_extractions": kwargs.get("total_successful", 0),
                "failed_extractions": kwargs.get("total_failed", 0),
                "success_rate": kwargs.get("success_rate", 0)
            },
            "batch_statistics": {
                "total_batches": kwargs.get("total_batches", 0),
                "successful_batches": len([br for br in kwargs.get("batch_results", []) if br.successful_extractions > 0]),
                "failed_batches": len([br for br in kwargs.get("batch_results", []) if br.failed_extractions > 0])
            },
            "performance_metrics": {
                "total_extraction_time": kwargs.get("extraction_time_seconds", 0),
                "average_time_per_article": kwargs.get("average_time_per_article", 0),
                "throughput_articles_per_hour": 3600 / kwargs.get("average_time_per_article", 1) if kwargs.get("average_time_per_article", 0) > 0 else 0
            },
            "error_analysis": {
                "total_errors": len(kwargs.get("error_details", [])),
                "error_types": self._analyze_error_types(kwargs.get("error_details", [])),
                "error_details": kwargs.get("error_details", [])
            },
            "model_configuration": {
                "model_name": self.extractor.model_name if self.extractor else "unknown",
                "temperature": self.extractor.temperature if self.extractor else 0.1,
                "max_tokens": self.extractor.max_tokens if self.extractor else 1024,
                "batch_size": self.extraction_service.batch_size,
                "delay_seconds": kwargs.get("delay_seconds", 1.0)
            }
        }
    
    def _save_comprehensive_summary(self, session_id: str, summary: Dict[str, Any], target_date: str):
        """Save comprehensive summary with full details"""
        try:
            output_dir = Path("data/extracted") / target_date / session_id
            output_dir.mkdir(parents=True, exist_ok=True)
            
            summary_file = output_dir / "comprehensive_summary.json"
            metadata_file = output_dir / "processing_metadata.json"
            
            # Save comprehensive summary
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
            
            # Save processing metadata
            metadata = {
                "session_id": session_id,
                "target_date": target_date,
                "processor_version": "1.0.0",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "log_files": self._get_log_files(target_date)
            }
            
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
            
            self.logger.info(f"💾 Saved comprehensive summary to {summary_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save comprehensive summary: {e}")
    
    def _get_log_files(self, target_date: str) -> List[str]:
        """Get list of log files for the date"""
        log_dir = Path("data/extracted/logs")
        if log_dir.exists():
            pattern = f"*{target_date}*"
            return [str(f) for f in log_dir.glob(pattern)]
        return []
    
    def _analyze_error_types(self, errors: List[str]) -> Dict[str, int]:
        """Analyze error patterns and categorize them"""
        error_types = {}
        
        for error in errors:
            # Common error patterns
            if "Invalid API key" in error:
                error_types["api_key_error"] = error_types.get("api_key_error", 0) + 1
            elif "rate limit" in error.lower():
                error_types["rate_limit_error"] = error_types.get("rate_limit_error", 0) + 1
            elif "timeout" in error.lower():
                error_types["timeout_error"] = error_types.get("timeout_error", 0) + 1
            elif "validation" in error.lower():
                error_types["validation_error"] = error_types.get("validation_error", 0) + 1
            elif "network" in error.lower():
                error_types["network_error"] = error_types.get("network_error", 0) + 1
            elif "parsing" in error.lower():
                error_types["parsing_error"] = error_types.get("parsing_error", 0) + 1
            else:
                error_types["other_error"] = error_types.get("other_error", 0) + 1
        
        return error_types