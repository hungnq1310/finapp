"""
LLM Extractor Agent for Financial News Analysis

This module provides intelligent extraction of structured financial information
from news articles using Large Language Models via OpenRouter.
"""

import json
import logging
import os
import time
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timezone

import jinja2
from langchain_openai import ChatOpenAI
from pydantic import ValidationError

from ...config import Config
from ...schema.extractor import (
    FinancialNewsExtraction,
    ExtractionBatchResult,
    ExtractionSession
)
from ...schema.request import LLMExtractorResponse


logger = logging.getLogger(__name__)


class LLMExtractorAgent:
    """
    LLM-powered agent for extracting structured financial information from news articles.
    
    Uses OpenRouter API with configurable LLM models to analyze articles and extract
    structured data according to predefined schema for financial analysis.
    """
    
    def __init__(self, 
                 api_key: Optional[str] = None,
                 model_name: Optional[str] = None,
                 temperature: Optional[float] = None,
                 max_tokens: Optional[int] = None,
                 base_url: Optional[str] = None):
        """
        Initialize the LLM Extractor Agent
        
        Args:
            api_key: OpenRouter API key (defaults to Config.OPENROUTER_API_KEY)
            model_name: LLM model name (defaults to Config.LLM_MODEL_NAME)
            temperature: Sampling temperature (defaults to Config.LLM_TEMPERATURE)
            max_tokens: Maximum tokens (defaults to Config.LLM_MAX_TOKENS)
            base_url: API base URL (defaults to Config.OPENROUTER_BASE_URL)
        """
        self.api_key = api_key or Config.OPENROUTER_API_KEY
        self.model_name = model_name or Config.LLM_MODEL_NAME
        self.temperature = temperature or Config.LLM_TEMPERATURE
        self.max_tokens = max_tokens or Config.LLM_MAX_TOKENS
        self.base_url = base_url or Config.OPENROUTER_BASE_URL
        
        # Initialize Jinja2 environment for prompts
        # Use absolute path from current working directory
        current_dir = os.getcwd()
        prompts_dir = os.path.join(current_dir, 'prompts')
        
        # If prompts dir not found in current dir, try relative to this file
        if not os.path.exists(prompts_dir):
            # Go up 3 levels from src/finapp/services/extract/
            prompts_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'prompts')
            prompts_dir = os.path.abspath(prompts_dir)
        
        self.prompt_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(prompts_dir)
        )
        
        # Initialize LLM client
        self._init_llm_client()
        
        # Load prompt template
        self.prompt_template = self._load_prompt_template()
        
        # Load JSON schema
        self.json_schema = self._load_json_schema()
        
        logger.info(f"✅ LLMExtractorAgent initialized with model: {self.model_name}")
    
    def _init_llm_client(self) -> None:
        """Initialize the LangChain ChatOpenAI client"""
        if not self.api_key:
            raise ValueError("OpenRouter API key is required. Set OPENROUTER_API_KEY environment variable.")
        
        try:
            self.llm = ChatOpenAI(
                api_key=self.api_key,
                model=self.model_name,
                temperature=self.temperature,
                base_url=self.base_url,
                timeout=60.0
            )
            logger.info(f"✅ LLM client initialized: {self.model_name}")
            logger.info(f"⚠️ Note: max_tokens={self.max_tokens} is set in model configuration")
        except Exception as e:
            logger.error(f"❌ Failed to initialize LLM client: {e}")
            raise
    
    def _load_prompt_template(self) -> jinja2.Template:
        """Load the extraction prompt template"""
        try:
            template = self.prompt_env.get_template('extractor.j2')
            logger.info("✅ Prompt template loaded successfully")
            return template
        except Exception as e:
            logger.error(f"❌ Failed to load prompt template: {e}")
            raise
    
    def _load_json_schema(self) -> Dict[str, Any]:
        """Load the JSON schema for extraction"""
        try:
            # Try to find json-schema directory
            current_dir = os.getcwd()
            schema_path = os.path.join(current_dir, 'json-schema', 'extractor.json')
            
            # If not found, try relative to this file
            if not os.path.exists(schema_path):
                # Go up 3 levels from src/finapp/services/extract/
                schema_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'json-schema', 'extractor.json')
                schema_path = os.path.abspath(schema_path)
            
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)
            
            logger.info("✅ JSON schema loaded successfully")
            return schema
        except Exception as e:
            logger.error(f"❌ Failed to load JSON schema: {e}")
            # Return empty schema if loading fails
            return {}
    
    def extract_single_article(self, 
                             title: str,
                             category: str,
                             description_text: str,
                             main_content: str,
                             article_guid: str) -> FinancialNewsExtraction:
        """
        Extract structured information from a single article
        
        Args:
            title: Article title
            category: Article category
            description_text: Article description/summary
            main_content: Full article content
            article_guid: Unique article identifier
            
        Returns:
            FinancialNewsExtraction object with extracted structured data
            
        Raises:
            ValueError: If extraction fails or output is invalid
        """
        try:
            # Prepare prompt with JSON schema
            prompt = self.prompt_template.render(
                title=title,
                category=category,
                description_text=description_text,
                main_content=main_content,
                json_schema=json.dumps(self.json_schema, indent=2, ensure_ascii=False)
            )
            
            logger.info(f"🔄 Extracting data for article: {article_guid}")
            logger.info(f"📝 Title: {title[:100]}...")
            logger.info(f"📝 Category: {category}")
            logger.info(f"📝 Description length: {len(description_text)} chars")
            logger.info(f"📝 Main content length: {len(main_content)} chars")
            logger.debug(f"📝 Prompt length: {len(prompt)} characters")
            logger.debug(f"📝 First 500 chars of main content: {main_content[:500]}...")
            start_time = time.time()
            
            # Call LLM
            response = self.llm.invoke(prompt)
            extraction_time = time.time() - start_time
            
            logger.debug(f"🤖 LLM response received in {extraction_time:.2f}s")
            logger.debug(f"🤖 Response length: {len(str(response.content))} characters")
            logger.info(f"🤖 Raw LLM response: {response.content}")
            logger.debug(f"🤖 First 500 chars of response: {str(response.content)[:500]}...")
            
            # Parse response
            extracted_data = self._parse_llm_response(str(response.content))
            logger.info(f"📋 Parsed extracted data keys: {list(extracted_data.keys())}")
            logger.info(f"📋 Stock tickers found: {len(extracted_data.get('stock_tickers', []))}")
            logger.info(f"📋 Sectors found: {len(extracted_data.get('sectors_industries', []))}")
            logger.info(f"📋 Overall sentiment: {extracted_data.get('sentiment_analysis', {}).get('overall_sentiment', 'N/A')}")
            logger.info(f"📋 Market impact scope: {extracted_data.get('market_impact', {}).get('impact_scope', 'N/A')}")
            
            # Add metadata
            extracted_data.update({
                'article_guid': article_guid,
                'article_title': title,
                'article_category': category,
                'extraction_timestamp': datetime.now(timezone.utc),
                'extraction_model': self.model_name,
                'extraction_confidence': self._calculate_confidence_score(extracted_data)
            })
            
            # Validate and create model
            extraction_result = FinancialNewsExtraction(**extracted_data)
            
            logger.info(f"✅ Extraction completed for {article_guid} in {extraction_time:.2f}s")
            return extraction_result
            
        except ValidationError as e:
            logger.error(f"❌ Validation error for article {article_guid}: {e}")
            raise ValueError(f"Invalid extraction format: {e}")
        except Exception as e:
            logger.error(f"❌ Extraction failed for article {article_guid}: {e}")
            raise ValueError(f"Extraction failed: {e}")
    
    def extract_batch(self, 
                     articles: List[Dict[str, Any]],
                     delay_seconds: Optional[float] = None) -> ExtractionBatchResult:
        """
        Extract structured information from multiple articles in batch
        
        Args:
            articles: List of article dictionaries with required fields
            delay_seconds: Delay between extractions (defaults to Config.EXTRACTOR_DELAY_SECONDS)
            
        Returns:
            ExtractionBatchResult with batch processing results
        """
        delay = delay_seconds or Config.EXTRACTOR_DELAY_SECONDS
        start_time = time.time()
        
        results = []
        errors = []
        successful_count = 0
        failed_count = 0
        
        logger.info(f"🚀 Starting batch extraction for {len(articles)} articles")
        
        for i, article in enumerate(articles, 1):
            try:
                # Extract required fields
                required_fields = ['title', 'category', 'description_text', 'main_content', 'guid']
                missing_fields = [field for field in required_fields if field not in article]
                
                if missing_fields:
                    error_msg = f"Missing required fields: {missing_fields}"
                    errors.append(f"Article {i}: {error_msg}")
                    failed_count += 1
                    logger.warning(f"⚠️ Skipping article {i}: {error_msg}")
                    continue
                
                # Extract article data
                result = self.extract_single_article(
                    title=article['title'],
                    category=article['category'],
                    description_text=article['description_text'],
                    main_content=article['main_content'],
                    article_guid=article['guid']
                )
                
                results.append(result)
                successful_count += 1
                logger.info(f"✅ Progress: {i}/{len(articles)} articles processed")
                
                # Rate limiting
                if i < len(articles) and delay > 0:
                    time.sleep(delay)
                
            except Exception as e:
                error_msg = f"Extraction failed: {str(e)}"
                errors.append(f"Article {i} ({article.get('guid', 'unknown')}): {error_msg}")
                failed_count += 1
                logger.error(f"❌ Article {i} failed: {error_msg}")
        
        extraction_time = time.time() - start_time
        
        batch_result = ExtractionBatchResult(
            total_articles=len(articles),
            successful_extractions=successful_count,
            failed_extractions=failed_count,
            extraction_time_seconds=extraction_time,
            results=results,
            errors=errors
        )
        
        logger.info(f"🎉 Batch extraction completed: {successful_count}/{len(articles)} successful in {extraction_time:.2f}s")
        return batch_result
    
    def _parse_llm_response(self, response_content: str) -> Dict[str, Any]:
        """
        Parse LLM response content to extract JSON
        
        Args:
            response_content: Raw response content from LLM
            
        Returns:
            Parsed dictionary with extracted data
            
        Raises:
            ValueError: If response cannot be parsed as valid JSON
        """
        try:
            logger.info(f"🔍 Starting JSON parsing from response")
            logger.info(f"🔍 Full response content: {response_content}")
            
            # Try to extract JSON from response
            content = response_content.strip()
            
            # Look for JSON content within response
            if '```json' in content:
                # Extract JSON from code block
                start = content.find('```json') + 7
                end = content.find('```', start)
                json_str = content[start:end].strip()
                logger.info("🔍 Found JSON in ```json code block")
            elif '```' in content:
                # Extract from generic code block
                start = content.find('```') + 3
                end = content.find('```', start)
                json_str = content[start:end].strip()
                logger.info("🔍 Found JSON in generic code block")
            else:
                # Assume entire response is JSON
                json_str = content
                logger.info("🔍 Assuming entire response is JSON")
            
            logger.info(f"🔍 Extracted JSON string (first 1000 chars): {json_str[:1000]}...")
            
            # Parse JSON
            parsed_data = json.loads(json_str)
            
            logger.info("✅ LLM response parsed successfully")
            logger.info(f"✅ Parsed data keys: {list(parsed_data.keys())}")
            
            # Check if all required keys are present with default values
            default_values_found = {
                'sentiment_analysis': parsed_data.get('sentiment_analysis', {}).get('overall_sentiment') == 'neutral',
                'stock_tickers': len(parsed_data.get('stock_tickers', [])) == 0,
                'sectors_industries': len(parsed_data.get('sectors_industries', [])) == 0,
                'market_impact': parsed_data.get('market_impact', {}).get('impact_scope') == 'not_mentioned',
            }
            
            logger.warning(f"⚠️ Default values detected: {default_values_found}")
            
            return parsed_data
            
        except json.JSONDecodeError as e:
            json_str_for_error = json_str if 'json_str' in locals() else 'N/A'
            logger.error(f"❌ JSONDecodeError: {e}")
            logger.error(f"❌ JSON string that failed (first 1000 chars): {json_str_for_error[:1000] if json_str_for_error != 'N/A' else 'N/A'}")
            logger.error(f"❌ Full response that failed: {response_content}")
            raise ValueError(f"Invalid JSON in LLM response: {e}")
        except Exception as e:
            logger.error(f"❌ General parsing error: {e}")
            logger.error(f"❌ Response content: {response_content[:1000]}...")
            raise ValueError(f"Response parsing failed: {e}")
    
    def _calculate_confidence_score(self, extracted_data: Dict[str, Any]) -> float:
        """
        Calculate confidence score based on completeness of extracted data
        
        Args:
            extracted_data: Dictionary with extracted data
            
        Returns:
            Confidence score between 0.0 and 1.0
        """
        try:
            # Check key fields presence
            required_sections = [
                'sentiment_analysis',
                'financial_indicators', 
                'market_impact',
                'news_classification'
            ]
            
            present_sections = sum(1 for section in required_sections if section in extracted_data)
            base_score = present_sections / len(required_sections)
            
            # Bonus points for detailed extractions
            bonus = 0.0
            
            # Stock tickers bonus
            stock_tickers = extracted_data.get('stock_tickers', [])
            if stock_tickers:
                bonus += min(len(stock_tickers) * 0.05, 0.2)
            
            # Numerical data bonus
            numerical_data = extracted_data.get('numerical_data', {})
            if numerical_data.get('has_specific_numbers', False):
                bonus += 0.1
            
            # Key events bonus
            key_events = extracted_data.get('key_events', [])
            if key_events:
                bonus += min(len(key_events) * 0.03, 0.15)
            
            confidence = min(base_score + bonus, 1.0)
            return round(confidence, 3)
            
        except Exception as e:
            logger.warning(f"⚠️ Error calculating confidence score: {e}")
            return 0.5  # Default confidence
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the current model configuration"""
        return {
            'model_name': self.model_name,
            'temperature': self.temperature,
            'max_tokens': self.max_tokens,
            'base_url': self.base_url,
            'api_configured': bool(self.api_key)
        }