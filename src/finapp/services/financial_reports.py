"""
Financial Report Generation Service using Longdoc System.

This service adapts the longdoc module for generating comprehensive financial reports
for stocks, sectors, or market analysis.
"""

import logging
import os
import uuid
import tempfile
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path

# Import longdoc components with proper error handling
import sys
import os
from pathlib import Path

# Add longdoc to Python path
longdoc_path = Path(__file__).parent.parent.parent.parent / "third-party" / "longdoc"
if longdoc_path.exists():
    sys.path.insert(0, str(longdoc_path))
    try:
        from src.config.config import APIConfig, QdrantConfig, LLMAgentConfig
        from src.qdrant import QdrantManager
        from src.agent.read import DocumentReadAgent
        from src.agent.write import DocumentWriteAgent
        from src.documents.chunking import Chunking
        from src.documents.embedding import Embedding
        LONGDOC_AVAILABLE = True
    except ImportError as e:
        print(f"Warning: Longdoc not available: {e}")
        LONGDOC_AVAILABLE = False
else:
    print("Warning: Longdoc directory not found")
    LONGDOC_AVAILABLE = False

from ..database.mongo import get_mongo_client
from ..api.routes.financial_reports import FinancialReportService

logger = logging.getLogger(__name__)


class FinancialReportGenerator:
    """
    Service for generating financial reports using the longdoc system.
    
    This service:
    1. Gathers relevant articles from MongoDB based on entity type (sector/ticker/market)
    2. Creates comprehensive documents suitable for longdoc processing
    3. Uses longdoc agents to generate structured financial reports
    4. Returns reports in markdown format
    """
    
    def __init__(self):
        """Initialize the financial report generator."""
        self.financial_service = FinancialReportService()
        self.longdoc_available = LONGDOC_AVAILABLE
        
        if not self.longdoc_available:
            logger.warning("Longdoc system not available - financial reports will be limited")
            return
        
        # Initialize longdoc configurations
        try:
            self.api_config = APIConfig.from_env()
            self.qdrant_config = QdrantConfig.from_env()
            self.llm_config = LLMAgentConfig.from_env()
            
            # Initialize longdoc components
            self.qdrant_manager = QdrantManager(self.qdrant_config)
            self.read_agent = DocumentReadAgent(self.llm_config, qdrant_config=self.qdrant_config)
            self.write_agent = DocumentWriteAgent(self.llm_config, qdrant_config=self.qdrant_config)
            self.chunking = Chunking(self.api_config)
            self.embedding = Embedding(self.api_config)
            
            logger.info("Financial report generator initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize financial report generator: {e}")
            self.longdoc_available = False
            logger.warning("Financial report generator running in limited mode")
    
    def generate_financial_report(
        self,
        entity_type: str,
        entity_value: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        include_sentiment: bool = True,
        include_stock_analysis: bool = True,
        include_sector_analysis: bool = True,
        include_market_analysis: bool = True,
        report_title: Optional[str] = None,
        output_filename: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a comprehensive financial report using the longdoc system.
        
        Args:
            entity_type: 'sector', 'ticker', or 'market'
            entity_value: sector name or ticker code (not required for 'market')
            date_from: Start date in YYYY-MM-DD format
            date_to: End date in YYYY-MM-DD format
            include_sentiment: Include sentiment analysis data
            include_stock_analysis: Include stock analysis data
            include_sector_analysis: Include sector analysis data
            include_market_analysis: Include market analysis data
            report_title: Custom title for the report
            output_filename: Custom filename for the output
            
        Returns:
            Dict containing the generated report and metadata
        """
        try:
            logger.info(f"Starting financial report generation for {entity_type}:{entity_value}")
            
            # Step 1: Gather relevant articles
            articles = self.financial_service.gather_articles_for_report(
                entity_type=entity_type,
                entity_value=entity_value,
                date_from=date_from,
                date_to=date_to,
                include_sentiment=include_sentiment,
                include_stock_analysis=include_stock_analysis,
                include_sector_analysis=include_sector_analysis,
                include_market_analysis=include_market_analysis
            )
            
            if not articles:
                return {
                    "success": False,
                    "message": "No articles found for the specified criteria",
                    "report": None,
                    "metadata": {
                        "entity_type": entity_type,
                        "entity_value": entity_value,
                        "articles_found": 0
                    }
                }
            
            logger.info(f"Found {len(articles)} articles for report generation")
            
            if not self.longdoc_available:
                # Fallback: Generate simple report without longdoc
                return self._generate_simple_report(articles, entity_type, entity_value)
            
            # Step 2: Create comprehensive document
            document_content = self._create_comprehensive_document(
                articles, entity_type, entity_value, report_title
            )
            
            # Step 3: Process with longdoc system
            report_result = self._process_with_longdoc(
                document_content, entity_type, entity_value, output_filename
            )
            
            # Step 4: Add metadata
            result = {
                "success": True,
                "message": f"Successfully generated {entity_type} report",
                "report": report_result["report"],
                "metadata": {
                    "entity_type": entity_type,
                    "entity_value": entity_value,
                    "articles_found": len(articles),
                    "date_from": date_from,
                    "date_to": date_to,
                    "generated_at": datetime.now().isoformat(),
                    "collections_used": list(set(article["source_collection"] for article in articles)),
                    "processing_stats": report_result.get("processing_stats", {})
                }
            }
            
            logger.info(f"Financial report generation completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Error generating financial report: {e}")
            return {
                "success": False,
                "message": f"Failed to generate report: {str(e)}",
                "report": None,
                "metadata": {
                    "entity_type": entity_type,
                    "entity_value": entity_value,
                    "error": str(e)
                }
            }
    
    def _create_comprehensive_document(
        self, 
        articles: List[Dict[str, Any]], 
        entity_type: str, 
        entity_value: Optional[str],
        custom_title: Optional[str] = None
    ) -> str:
        """Create a comprehensive document from articles for longdoc processing."""
        logger.info("Creating comprehensive document from articles")
        
        # Create title
        if custom_title:
            title = custom_title
        elif entity_type == "sector":
            title = f"Báo Cáo Phân Tích Ngành: {entity_value}"
        elif entity_type == "ticker":
            title = f"Báo Cổ Phiếu: {entity_value}"
        else:
            title = "Báo Cáo Tổng Quan Thị Trường"
        
        # Start building document
        content_parts = []
        
        # Document header
        header = f"# {title}\n\n"
        header += f"**Ngày tạo:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n"
        header += f"**Tổng số bài viết:** {len(articles)}\n"
        header += f"**Loại phân tích:** {entity_type}\n"
        if entity_value:
            header += f"**Đối tượng:** {entity_value}\n"
        header += "\n---\n\n"
        content_parts.append(header)
        
        # Executive summary
        content_parts.append("## Tóm Tắt Tổng Quan\n\n")
        content_parts.append(f"Báo cáo này tổng hợp và phân tích {len(articles)} bài viết liên quan đến ")
        if entity_type == "sector":
            content_parts.append(f"ngành {entity_value}")
        elif entity_type == "ticker":
            content_parts.append(f"cổ phiếu {entity_value}")
        else:
            content_parts.append("thị trường chứng khoán Việt Nam")
        
        content_parts.append(". Các phân tích bao gồm tâm lý thị trường, phân tích kỹ thuật, phân tích cơ bản và các tác động vĩ mô.\n\n")
        
        # Group articles by collection type
        by_collection = {}
        for article in articles:
            collection = article["source_collection"]
            if collection not in by_collection:
                by_collection[collection] = []
            by_collection[collection].append(article)
        
        # Add articles by collection
        collection_titles = {
            "llm_sentiment_analysis": "Phân Tích Tâm Lý Thị Trường",
            "llm_stock_analysis": "Phân Tích Cổ Phiếu",
            "llm_sector_analysis": "Phân Tích Ngành",
            "llm_market_analysis": "Phân Tích Thị Trường"
        }
        
        for collection, collection_articles in by_collection.items():
            section_title = collection_titles.get(collection, collection.replace("_", " ").title())
            content_parts.append(f"## {section_title}\n\n")
            
            for i, article in enumerate(collection_articles, 1):
                content_parts.append(f"### {i}. {article['title']}\n\n")
                
                # Add article metadata
                metadata = article["metadata"]
                if metadata.get("sectors"):
                    content_parts.append(f"**Ngành liên quan:** {', '.join(metadata['sectors'])}\n")
                if metadata.get("ticker"):
                    content_parts.append(f"**Mã cổ phiếu:** {metadata['ticker']}\n")
                if metadata.get("mentioned_stocks"):
                    stocks = []
                    for stock in metadata['mentioned_stocks']:
                        if isinstance(stock, dict) and 'ticker' in stock:
                            stocks.append(stock['ticker'])
                        elif isinstance(stock, str):
                            stocks.append(stock)
                    if stocks:
                        content_parts.append(f"**Các cổ phiếu đề cập:** {', '.join(stocks)}\n")
                if metadata.get("sentiment"):
                    content_parts.append(f"**Xu hướng:** {metadata['sentiment']}\n")
                
                content_parts.append(f"**Ngày đăng:** {article['created_at'][:10]}\n\n")
                
                # Add article content
                content_parts.append(f"**Nội dung phân tích:**\n\n{article['content']}\n\n")
                content_parts.append("---\n\n")
        
        # Add conclusions section
        content_parts.append("## Kết Luận Và Đề Xuất\n\n")
        content_parts.append("*[Phần này sẽ được hệ thống AI tự động phân tích và điền kết luận dựa trên nội dung các bài viết trên]*\n\n")
        
        document = "".join(content_parts)
        logger.info(f"Created comprehensive document ({len(document)} characters)")
        
        return document
    
    def _process_with_longdoc(
        self, 
        document_content: str, 
        entity_type: str, 
        entity_value: Optional[str],
        output_filename: Optional[str] = None
    ) -> Dict[str, Any]:
        """Process the document using longdoc system."""
        try:
            logger.info("Starting longdoc processing")
            
            # Create temporary file for the document
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as temp_file:
                temp_file.write(document_content)
                temp_file_path = temp_file.name
            
            try:
                # Step 1: Create semantic chunks
                logger.info("Creating semantic chunks")
                large_chunks = self.chunking._semantic_chunk_text(document_content)
                logger.info(f"Created {len(large_chunks)} large chunks")
                
                # Step 2: Create collection name
                collection_name = f"financial_{entity_type}"
                if entity_value:
                    # Sanitize entity value for collection name
                    safe_value = "".join(c for c in entity_value if c.isalnum()).lower()
                    collection_name += f"_{safe_value}"
                collection_name += f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                logger.info(f"Using collection name: {collection_name}")
                
                # Step 3: Process embeddings and store in Qdrant
                if not self.qdrant_manager.is_collection_exists(collection_name):
                    logger.info("Creating new Qdrant collection and embedding documents")
                    
                    # Create collection
                    from qdrant_client.http import models as qdrant_models
                    from src.qdrant.client import VectorParams
                    
                    vector_params = VectorParams(
                        size=768,
                        distance=qdrant_models.Distance.COSINE,
                        on_disk=True
                    )
                    
                    qdrant_client = self.qdrant_manager.client
                    qdrant_client.create_collection(
                        collection_name=collection_name,
                        vectors_config=vector_params,
                        force_recreate=False
                    )
                    
                    # Embed and upload chunks
                    all_smart_chunks = []
                    for i, chunk in enumerate(large_chunks):
                        logger.info(f"Embedding chunk {i+1}/{len(large_chunks)}")
                        smart_chunks = self.embedding._process_single_chunk(chunk, i)
                        all_smart_chunks.extend(smart_chunks)
                    
                    # Prepare metadata
                    embeddings = [chunk.embedding for chunk in all_smart_chunks]
                    metadata_list = []
                    chunk_texts = []
                    
                    doc_id = str(uuid.uuid4())
                    for i, smart_chunk in enumerate(all_smart_chunks):
                        from src.qdrant.manager import DocumentMetadata
                        
                        metadata = DocumentMetadata(
                            doc_id=doc_id,
                            title=f"Financial Report {entity_type}:{entity_value}",
                            source="financial_report_generator",
                            document_type="financial_analysis",
                            chunk_index=i,
                            total_chunks=len(all_smart_chunks),
                            created_at=datetime.now().isoformat(),
                            updated_at=datetime.now().isoformat(),
                            tags=["financial", entity_type, entity_value or "market"]
                        )
                        metadata_list.append(metadata)
                        chunk_texts.append(smart_chunk.chunk)
                    
                    # Upload to Qdrant
                    self.qdrant_manager.collection_name = collection_name
                    success = self.qdrant_manager.add_document(embeddings, metadata_list, chunk_texts)
                    
                    if not success:
                        raise Exception("Failed to upload chunks to Qdrant")
                    
                    logger.info(f"Successfully uploaded {len(all_smart_chunks)} chunks to Qdrant")
                else:
                    logger.info(f"Collection {collection_name} already exists, skipping embedding")
                
                # Step 4: Create batches for read agent
                batches = self._create_batches_from_chunks(large_chunks, max_batch_size=5000)
                logger.info(f"Created {len(batches)} batches for processing")
                
                # Step 5: Process with read agent
                logger.info("Processing with Read Agent")
                document_id = str(uuid.uuid4())
                skeleton = None
                
                for i, batch in enumerate(batches):
                    logger.info(f"Processing batch {i+1}/{len(batches)}")
                    
                    try:
                        skeleton = self.read_agent.analyze_document_chunk(
                            chunk_text=batch,
                            document_id=document_id,
                            chunk_index=i,
                            existing_skeleton=skeleton
                        )
                        
                        if i == 0:
                            logger.info(f"Initial skeleton created with {len(skeleton.main_sections)} sections")
                        else:
                            logger.info(f"Skeleton updated (version {skeleton.version})")
                            
                    except Exception as e:
                        logger.error(f"Error processing batch {i+1}: {e}")
                        if i == 0:
                            raise
                
                if not skeleton:
                    raise Exception("Failed to create report skeleton")
                
                logger.info(f"Read Agent completed. Final skeleton has {len(skeleton.main_sections)} sections")
                
                # Step 6: Process with write agent
                logger.info("Processing with Write Agent")
                
                # Generate output filename
                if not output_filename:
                    base_name = f"financial_report_{entity_type}"
                    if entity_value:
                        base_name += f"_{entity_value.replace(' ', '_')}"
                    output_filename = base_name
                
                complete_report = self.write_agent.write_complete_report(
                    skeleton=skeleton,
                    collection_name=collection_name,
                    context_limit=5,
                    output_filename=output_filename
                )
                
                logger.info("Write Agent completed successfully")
                
                # Step 7: Gather processing stats
                processing_stats = {
                    "total_chunks": len(large_chunks),
                    "total_batches": len(batches),
                    "skeleton_sections": len(skeleton.main_sections),
                    "report_sections": len(complete_report.main_sections),
                    "collection_name": collection_name,
                    "document_id": document_id
                }
                
                return {
                    "report": {
                        "title": complete_report.title,
                        "sections": [
                            {
                                "order": section.order,
                                "title": section.title,
                                "description": section.description,
                                "content_length": len(section.content) if section.content else 0
                            }
                            for section in sorted(complete_report.main_sections, key=lambda x: x.order)
                        ],
                        "version": complete_report.version,
                        "created_at": complete_report.created_at,
                        "updated_at": complete_report.updated_at
                    },
                    "processing_stats": processing_stats
                }
                
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file_path)
                except:
                    pass
                    
        except Exception as e:
            logger.error(f"Error in longdoc processing: {e}")
            raise
    
    def _create_batches_from_chunks(self, chunks: List[str], max_batch_size: int = 5000) -> List[str]:
        """Create batches of chunks with total size <= max_batch_size."""
        batches = []
        current_batch = []
        current_length = 0
        
        for chunk in chunks:
            chunk_length = len(chunk)
            
            if current_length + chunk_length > max_batch_size and current_batch:
                batches.append(" ".join(current_batch))
                current_batch = [chunk]
                current_length = chunk_length
            else:
                current_batch.append(chunk)
                current_length += chunk_length
        
        if current_batch:
            batches.append(" ".join(current_batch))
        
        return batches
    
    def get_available_entities(self, days_back: int = 30) -> Dict[str, Any]:
        """Get available sectors and tickers for report generation."""
        return self.financial_service.get_available_entities(days_back)
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check of the financial report generator."""
        try:
            # Check longdoc components
            read_agent_health = self.read_agent.health_check()
            write_agent_health = self.write_agent.health_check()
            qdrant_health = self.qdrant_manager.health_check()
            
            # Check financial service
            available_entities = self.get_available_entities(7)  # Quick check with 7 days
            
            return {
                "status": "healthy" if all([
                    read_agent_health["agent"] == "healthy",
                    write_agent_health["agent"] == "healthy",
                    qdrant_health
                ]) else "degraded",
                "components": {
                    "read_agent": read_agent_health,
                    "write_agent": write_agent_health,
                    "qdrant": "connected" if qdrant_health else "error",
                    "financial_service": "connected" if available_entities else "error"
                },
                "capabilities": {
                    "sectors_available": len(available_entities.available_sectors),
                    "tickers_available": len(available_entities.available_tickers),
                    "total_articles": available_entities.total_articles
                }
            }
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "components": {},
                "capabilities": {}
            }