"""
Auto Processor for Enhanced LLM Extraction

This module provides enhanced processing with detailed logging,
error handling, and recovery mechanisms for LLM extraction.
"""

import json
import logging
import os
import time as time_module
from datetime import datetime, timezone, time as datetime_time
from pathlib import Path
from typing import Dict, List, Any, Optional

from ...services.extract.extraction_service import ExtractionService
from ...services.extract.extrator_agent import LLMExtractorAgent
from ...services.extract.html_content import HTMLContentExtractor
from ...services.crawl.models import Article
from ...schema.extractor import FinancialNewsExtraction, ExtractionBatchResult
from src.finapp.database.llm_extraction import LLMExtractionRepository
from src.finapp.config import Config


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
        
        logger.info("AutoProcessor initialized with enhanced logging")
    
    def setup_enhanced_logging(self):
        """Setup enhanced logging configuration"""
        # Create dedicated log directory from config
        log_dir = Path(Config.LOG_DIR)
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
            self.logger.info(f"LLM Extractor initialized: {self.extractor.model_name}")
            return self.extractor
        except Exception as e:
            self.logger.error(f"Failed to initialize LLM Extractor: {e}")
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
        start_time = time_module.time()
        
        self.logger.info("=" * 80)
        self.logger.info(f"Starting Auto-Processing for Date: {target_date}")
        self.logger.info(f"Session ID: {session_id}")
        self.logger.info(f"Delay between articles: {delay_seconds}s")
        self.logger.info("=" * 80)
        
        try:
            # Step 1: Check if JSON file exists
            vietstock_output_dir = Config.CRAWLER_SOURCE_CONFIGS['vietstock']['output_dir']
            json_file = Path(f"{vietstock_output_dir}/{target_date}/articles_{target_date}.json")
            
            if not json_file.exists():
                error_msg = f"JSON file not found: {json_file}. Please run crawling first!"
                self.logger.error(error_msg)
                return {
                    "success": False,
                    "message": error_msg,
                    "target_date": target_date,
                    "action_required": "Run crawl endpoint first: POST /crawl/trigger or POST /crawl/start"
                }
            
            self.logger.info(f"JSON file found: {json_file}")
            
            # Step 2: Load articles from JSON file
            self.logger.info(f"Loading articles from: {json_file}")
            
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            articles = data.get('articles', [])
            self.logger.info(f"Found {len(articles)} articles in JSON file")
            
            # Step 3: Check what's already processed in MongoDB
            processed_guids = self._get_processed_guids_for_date(target_date)
            available_guids = {article.get('guid', '') for article in articles if article.get('guid')}
            
            unprocessed_count = len(available_guids - processed_guids)
            processed_count = len(available_guids & processed_guids)
            
            self.logger.info(f"Processing Status:")
            self.logger.info(f"   - Available articles: {len(available_guids)}")
            self.logger.info(f"   - Already processed: {processed_count}")
            self.logger.info(f"   - Need to process: {unprocessed_count}")
            
            # Step 4: Initialize extractor
            extractor = self._initialize_extractor()
            
            # Check and extract HTML for articles that don't have it
            articles_without_html = []
            articles_with_html = []
            
            for article in articles:
                if not article.get('html_extraction_success', False) or not article.get('main_content'):
                    articles_without_html.append(article)
                else:
                    articles_with_html.append(article)
            
            if articles_without_html:
                self.logger.info(f"Found {len(articles_without_html)} articles without HTML, attempting to extract HTML content")
                html_extractor_service = HTMLContentExtractor()
                
                for article in articles_without_html:
                    try:
                        article_url = article.get('link', '')  # Use 'link' field instead of 'url'
                        if article_url:
                            # Create Article object for HTML extraction
                            article_obj = Article(
                                title=article.get('title', ''),
                                link=article_url,
                                description=article.get('description', ''),
                                pub_date=article.get('pub_date'),
                                source=article.get('source', 'vietstock'),
                                guid=article.get('guid', ''),
                                crawled_at=article.get('crawled_at')
                            )
                            
                            # Extract HTML content using Article object
                            extraction_result = html_extractor_service.extract_article_content(article_obj)
                            if extraction_result and extraction_result.get('extraction_success'):
                                article['html_extraction_success'] = True
                                article['main_content'] = extraction_result.get('main_content', '')
                                article['extraction_title'] = extraction_result.get('title', article.get('title', ''))
                                article['raw_html'] = extraction_result.get('raw_html', '')
                                self.logger.info(f"Successfully extracted HTML for article: {article.get('title', 'No title')[:50]}...")
                                articles_with_html.append(article)
                            else:
                                self.logger.warning(f"Failed to extract HTML for article: {article_url} - {extraction_result.get('error', 'Unknown error') if extraction_result else 'No result'}")
                        else:
                            self.logger.warning(f"No URL found for article: {article.get('title', 'No title')}")
                    except Exception as e:
                        self.logger.error(f"Error extracting HTML for article: {e}")
                        continue
            
            self.logger.info(f"Articles with HTML content after extraction: {len(articles_with_html)}")
            
            # Filter only unprocessed articles
            unprocessed_articles = [
                article for article in articles_with_html
                if article.get('guid', '') not in processed_guids
            ]
            
            self.logger.info(f"Unprocessed articles: {len(unprocessed_articles)}")
            
            if not unprocessed_articles:
                success_msg = f"All articles have already been processed for date {target_date}"
                self.logger.info(f"{success_msg}")
                return {
                    "success": True,
                    "message": success_msg,
                    "target_date": target_date,
                    "session_id": session_id,
                    "total_articles": len(articles),
                    "html_articles": len(articles_with_html),
                    "processed_articles": len(articles_with_html),  # All processed
                    "unprocessed_articles": 0,
                    "successful_extractions": 0,
                    "failed_extractions": 0,
                    "extraction_time_seconds": 0,
                    "status": "already_completed"
                }
            
            if not articles_with_html:
                warning_msg = f"No articles with HTML content found for date {target_date}"
                self.logger.warning(f"{warning_msg}")
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
            
            # Convert to required format (using unprocessed articles only)
            articles_to_process = []
            for article in unprocessed_articles:
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

            self.logger.info(f"Processing {len(articles_to_process)} articles in {total_batches} batches of {batch_size}")

            for i in range(0, len(articles_to_process), batch_size):
                batch_num = i + 1
                batch = articles_to_process[i:i + batch_size]
                
                self.logger.info(f"Processing Batch {batch_num}/{total_batches} ({len(batch)} articles)")
                
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
                    
                    self.logger.info(f"Batch {batch_num} completed: {batch_result.successful_extractions}/{batch_result.total_articles} successful")
                    
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
                            self.logger.error(f"Article Error: {error}")
                            error_details.append(error)
                    
                    if batch_result.results:
                        for result in batch_result.results:
                            self.logger.info(f"Article Extracted: {result.article_title[:50]}...")
                            self.logger.debug(f"   - GUID: {result.article_guid}")
                            self.logger.debug(f"   - Confidence: {result.extraction_confidence}")
                            self.logger.debug(f"   - Model: {result.extraction_model}")
                            self.logger.debug(f"   - Stock Tickers: {len(result.stock_level)}")
                            self.logger.debug(f"   - Sectors: {len(result.sector_level)}")
                    
                except Exception as e:
                    error_msg = f"Batch {batch_num} processing failed: {str(e)}"
                    self.logger.error(f"{error_msg}")
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
                    time_module.sleep(delay_seconds)
            
            # Calculate final statistics
            extraction_time = time_module.time() - start_time
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
                html_articles=len(articles_with_html),
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
            
            # Save extracted data to MongoDB
            try:
                self.logger.info(f"Starting MongoDB save process for {target_date}")
                save_result = self._save_extracted_data_to_mongodb(batch_results, target_date)
                self.logger.info(f"MongoDB save result: {save_result}")
                
                # Verify data was actually saved
                self._verify_mongodb_save(target_date)
                
            except Exception as e:
                self.logger.error(f"Failed to save extracted data to MongoDB: {e}")
                import traceback
                self.logger.error(f"Full MongoDB save error traceback: {traceback.format_exc()}")
                # Don't fail the entire process if MongoDB save fails
            
            self.logger.info("=" * 80)
            self.logger.info(f"Auto-Processing Completed for {target_date}")
            self.logger.info(f"Final Statistics:")
            self.logger.info(f"   - Total Articles: {len(articles_to_process)}")
            self.logger.info(f"   - Articles with HTML: {len(articles_with_html)}")
            
            # Update session before removing from active sessions (FIXED)
            if session_id in self.extraction_service.active_sessions:
                session = self.extraction_service.active_sessions[session_id]
                session.status = "completed"
                session.end_time = datetime.now(timezone.utc)
                session.total_articles = len(articles_to_process)
                self.extraction_service._save_session(session)
            
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
                "html_articles": len(articles_with_html),
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
            self.logger.error(f"Auto-processing failed for {target_date}: {e}")
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
        batch_start_time = time_module.time()
        
        self.logger.debug(f"Starting batch {batch_num} processing")
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
            
            batch_time = time_module.time() - batch_start_time
            
            self.logger.debug(f"Batch {batch_num} completed in {batch_time:.2f}s")
            self.logger.debug(f"   - Success: {batch_result.successful_extractions}")
            self.logger.debug(f"   - Failed: {batch_result.failed_extractions}")
            self.logger.debug(f"   - Success Rate: {batch_result.success_rate:.1f}%")
            
            return batch_result
            
        except Exception as e:
            self.logger.error(f"Batch {batch_num} failed: {e}")
            self.logger.exception("Batch processing error details:")
            
            # Create failed batch result
            failed_result = ExtractionBatchResult(
                total_articles=len(batch),
                successful_extractions=0,
                failed_extractions=len(batch),
                extraction_time_seconds=time_module.time() - batch_start_time,
                results=[],
                errors=[f"Batch {batch_num} failed: {str(e)}"]
            )
            
            return failed_result
    
    def _save_batch_results(self, session_id: str, batch_num: int, batch_result: ExtractionBatchResult, target_date: str):
        """Save batch results with enhanced metadata"""
        try:
            output_dir = Path(Config.EXTRACTOR_OUTPUT_DIR) / target_date / session_id
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
            
            self.logger.debug(f"Saved batch {batch_num} results to {batch_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save batch {batch_num} results: {e}")
    
    def _generate_comprehensive_summary(self, **kwargs) -> Dict[str, Any]:
        """Generate comprehensive processing summary with extracted articles data"""
        # Collect all extracted articles from batch results
        extracted_articles = []
        batch_results = kwargs.get("batch_results", [])
        
        for batch in batch_results:
            if hasattr(batch, 'results') and batch.results:
                for result in batch.results:
                    # Convert Pydantic model to dict for JSON serialization
                    if hasattr(result, 'dict'):
                        article_data = result.dict()
                    else:
                        article_data = result
                    
                    extracted_articles.append(article_data)
        
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
            "extracted_articles": extracted_articles,  # NEW: Include all extracted data
            "batch_statistics": {
                "total_batches": kwargs.get("total_batches", 0),
                "successful_batches": len([br for br in batch_results if br.successful_extractions > 0]),
                "failed_batches": len([br for br in batch_results if br.failed_extractions > 0])
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
            output_dir = Path(Config.EXTRACTOR_OUTPUT_DIR) / target_date / session_id
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
            
            self.logger.info(f"Saved comprehensive summary to {summary_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save comprehensive summary: {e}")
    
    def _save_extracted_data_to_mongodb(self, batch_results: List[ExtractionBatchResult], target_date: str) -> Dict[str, Any]:
        """Save all extracted data from batches to MongoDB collections"""
        try:
            extraction_service = LLMExtractionRepository()
            
            # Collect all successful extractions from all batches
            all_successful_extractions = []
            total_articles = 0
            total_successful = 0
            total_failed = 0
            
            self.logger.info(f"Processing {len(batch_results)} batches for MongoDB save")
            
            for batch_idx, batch_result in enumerate(batch_results):
                # Handle both dict and ExtractionBatchResult object
                if hasattr(batch_result, 'results'):
                    # It's an ExtractionBatchResult Pydantic model
                    results_list = batch_result.results
                    batch_successful = batch_result.successful_extractions
                    batch_failed = batch_result.failed_extractions
                elif isinstance(batch_result, dict) and 'results' in batch_result:
                    # It's a dict with results key
                    results_list = batch_result['results']
                    batch_successful = batch_result.get('successful_extractions', 0)
                    batch_failed = batch_result.get('failed_extractions', 0)
                else:
                    self.logger.warning(f"Batch {batch_idx}: Unknown batch_result structure, skipping")
                    continue
                
                self.logger.debug(f"Batch {batch_idx}: {len(results_list) if results_list else 0} results, {batch_successful} successful, {batch_failed} failed")
                total_articles += batch_successful + batch_failed
                total_successful += batch_successful
                total_failed += batch_failed
                
                # Process results list
                if results_list:
                    for result in results_list:
                        # Convert Pydantic model to dict if needed
                        if hasattr(result, 'dict'):
                            result_dict = result.dict()
                        elif hasattr(result, 'model_dump'):
                            result_dict = result.model_dump()
                        else:
                            result_dict = result
                        
                        # Extract necessary fields with safe access
                        article_guid = result_dict.get('article_guid') if isinstance(result_dict, dict) else getattr(result, 'article_guid', None)
                        article_title = result_dict.get('article_title') if isinstance(result_dict, dict) else getattr(result, 'article_title', None)
                        article_url = result_dict.get('article_url') if isinstance(result_dict, dict) else getattr(result, 'article_url', None)
                        extraction_timestamp = result_dict.get('extraction_timestamp') if isinstance(result_dict, dict) else getattr(result, 'extraction_timestamp', None)
                        extraction_model = result_dict.get('extraction_model') if isinstance(result_dict, dict) else getattr(result, 'extraction_model', None)
                        extraction_confidence = result_dict.get('extraction_confidence') if isinstance(result_dict, dict) else getattr(result, 'extraction_confidence', None)
                        
                        all_successful_extractions.append({
                            'article_guid': article_guid,
                            'article_title': article_title,
                            'article_url': article_url,
                            'extraction_result': result_dict,  # Full extraction result
                            'extraction_metadata': {
                                'extraction_timestamp': extraction_timestamp,
                                'extraction_model': extraction_model,
                                'extraction_confidence': extraction_confidence
                            }
                        })
            
            self.logger.info(f"MongoDB save stats - Total: {total_articles}, Successful: {total_successful}, Failed: {total_failed}")
            self.logger.info(f"Collected {len(all_successful_extractions)} extractions to save")
            
            if not all_successful_extractions:
                self.logger.warning("No successful extractions to save to MongoDB")
                return {"success": False, "reason": "no_successful_extractions", "total_articles": total_articles}
            
            self.logger.info(f"Attempting to save {len(all_successful_extractions)} successful extractions to MongoDB")
            
            # Save each extraction using the extraction service
            saved_count = 0
            failed_count = 0
            error_details = []
            
            for i, extraction_data in enumerate(all_successful_extractions):
                try:
                    article_guid = extraction_data['article_guid']
                    self.logger.debug(f"Saving extraction {i+1}/{len(all_successful_extractions)}: {article_guid}")
                    
                    # Prepare extraction result for MongoDB
                    # The extraction_result is a dict from FinancialNewsExtraction.dict()
                    extraction_result = extraction_data['extraction_result']
                    
                    # Ensure we have the required metadata fields for save_complete_extraction
                    if not extraction_result.get('extraction_timestamp'):
                        extraction_result['extraction_timestamp'] = extraction_data['extraction_metadata'].get('extraction_timestamp') or datetime.now(timezone.utc)
                    if not extraction_result.get('extraction_model'):
                        extraction_result['extraction_model'] = extraction_data['extraction_metadata'].get('extraction_model') or 'unknown'
                    if not extraction_result.get('extraction_confidence'):
                        extraction_result['extraction_confidence'] = extraction_data['extraction_metadata'].get('extraction_confidence') or 0.0
                    if not extraction_result.get('article_title'):
                        extraction_result['article_title'] = extraction_data['article_title'] or ''
                    if not extraction_result.get('article_guid'):
                        extraction_result['article_guid'] = article_guid
                    
                    # Add custom metadata
                    extraction_result['article_metadata'] = {
                        'article_url': extraction_data['article_url'],
                        'target_date': target_date
                    }
                    
                    # Save complete extraction (returns bool, not ID)
                    save_success = extraction_service.save_complete_extraction(
                        article_guid=article_guid,
                        extraction_result=extraction_result
                    )
                    
                    if save_success:
                        saved_count += 1
                        self.logger.debug(f"Successfully saved extraction {i+1}")
                    else:
                        failed_count += 1
                        error_details.append(f"Extraction {i+1}: save_complete_extraction returned False")
                        self.logger.warning(f"save_complete_extraction returned False for {article_guid}")
                    
                except Exception as e:
                    failed_count += 1
                    error_msg = f"Extraction {i+1} ({extraction_data.get('article_guid', 'unknown')}): {str(e)}"
                    error_details.append(error_msg)
                    self.logger.error(f"Failed to save extraction {i+1}: {e}")
                    import traceback
                    self.logger.debug(traceback.format_exc())
                    continue
            
            result = {
                "success": saved_count > 0,
                "total_extractions": len(all_successful_extractions),
                "saved_count": saved_count,
                "failed_count": failed_count,
                "error_details": error_details
            }
            
            self.logger.info(f"MongoDB save completed - Saved: {saved_count}/{len(all_successful_extractions)}")
            
            if failed_count > 0:
                self.logger.warning(f"Failed to save {failed_count} extractions to MongoDB")
                self.logger.debug(f"Error details: {error_details}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in _save_extracted_data_to_mongodb: {e}")
            import traceback
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            raise
    
    def _verify_mongodb_save(self, target_date: str):
        """Verify that data was actually saved to MongoDB"""
        try:
            repo = LLMExtractionRepository(Config.MONGODB_URI, Config.DATABASE_NAME)
            
            # Parse target_date to datetime range for proper querying
            target_date_obj = datetime.strptime(target_date, '%Y%m%d')
            start_date = datetime.combine(target_date_obj, datetime_time.min)
            end_date = datetime.combine(target_date_obj, datetime_time.max)
            
            self.logger.debug(f"Verifying MongoDB data for date range: {start_date} to {end_date}")
            
            # Check each collection for today's data
            collections = ['llm_sentiment_analysis', 'llm_stock_analysis', 'llm_sector_analysis', 'llm_market_analysis', 'llm_extractions']
            
            total_saved = 0
            for collection_name in collections:
                try:
                    db = repo.db
                    if db is None:
                        self.logger.error("MongoDB database connection is None")
                        continue
                    collection = db[collection_name]
                    
                    # Query by datetime range, not regex
                    count = collection.count_documents({
                        'created_at': {
                            '$gte': start_date,
                            '$lte': end_date
                        }
                    })
                    
                    total_saved += count
                    self.logger.info(f"MongoDB verification - {collection_name}: {count} documents saved for {target_date}")
                    
                    # Debug: Show sample document
                    if count > 0:
                        sample = collection.find_one({'created_at': {'$gte': start_date, '$lte': end_date}})
                        if sample:
                            self.logger.debug(f"Sample document from {collection_name}: article_guid={sample.get('article_guid', 'N/A')}, created_at={sample.get('created_at', 'N/A')}")
                    
                except Exception as e:
                    self.logger.warning(f"Error checking {collection_name}: {e}")
                    import traceback
                    self.logger.debug(traceback.format_exc())
            
            self.logger.info(f"MongoDB verification complete - Total documents saved: {total_saved}")
            
            if total_saved == 0:
                self.logger.error("❌ NO DATA WAS SAVED TO MONGODB!")
                # Additional debug: Check if ANY documents exist
                try:
                    db = repo.db
                    if db is not None:
                        for collection_name in collections:
                            total_count = db[collection_name].count_documents({})
                            self.logger.debug(f"Total documents in {collection_name}: {total_count}")
                except Exception as e:
                    self.logger.debug(f"Error checking total counts: {e}")
            else:
                self.logger.info("✅ Data successfully saved to MongoDB")
                
        except Exception as e:
            self.logger.error(f"Error during MongoDB verification: {e}")

    
    def _get_log_files(self, target_date: str) -> List[str]:
        """Get list of log files for the date"""
        log_dir = Path(Config.LOG_DIR)
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
    
    def _get_processed_guids_for_date(self, target_date: str) -> set:
        """Get set of processed article GUIDs for a specific date from MongoDB"""
        try:           
            llm_repo = LLMExtractionRepository(Config.MONGODB_URI, Config.DATABASE_NAME)
            
            db = llm_repo.db
            if db is None:
                self.logger.warning("MongoDB database connection is None, returning empty set")
                return set()
            
            master_collection = db.llm_extractions
            
            # Parse target_date to datetime range
            target_date_obj = datetime.strptime(target_date, '%Y%m%d')
            start_date = datetime.combine(target_date_obj, datetime_time.min)
            end_date = datetime.combine(target_date_obj, datetime_time.max)
            
            # Get processed GUIDs for this date from multiple sources
            processed_guids = set()
            
            # 1. Check master collection first
            master_docs = list(master_collection.find({
                'article_metadata.target_date': target_date
            }, {'article_metadata.article_guid': 1}))
            
            for doc in master_docs:
                guid = doc.get('article_metadata', {}).get('article_guid')
                if guid:
                    processed_guids.add(guid)
            
            # 2. Also check individual collections for any missing GUIDs
            collections_to_check = ['llm_sentiment_analysis', 'llm_stock_analysis', 'llm_sector_analysis', 'llm_market_analysis']
            
            for collection_name in collections_to_check:
                try:
                    collection = db[collection_name]
                    # Query with datetime objects, not ISO strings
                    docs = list(collection.find({
                        'created_at': {'$gte': start_date, '$lte': end_date}
                    }, {'article_guid': 1}))
                    
                    for doc in docs:
                        guid = doc.get('article_guid')
                        if guid:
                            processed_guids.add(guid)
                            
                except Exception as e:
                    self.logger.warning(f"Error checking {collection_name}: {e}")
                    continue
            
            self.logger.info(f"Found {len(processed_guids)} already processed GUIDs for {target_date}")
            return processed_guids
            
        except Exception as e:
            self.logger.warning(f"Could not get processed GUIDs from MongoDB: {e}")
            return set()