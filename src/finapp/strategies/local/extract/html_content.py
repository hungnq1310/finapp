"""
HTML Content Extractor Service

This service provides functionality to extract raw HTML content from article URLs.
It's designed to be extensible and maintainable with proper error handling and configuration.
"""

import requests
import logging
from typing import Optional, Dict, Any
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import time
import hashlib

from finapp.strategies.local.crawl.models import Article
from finapp.config import Config

logger = logging.getLogger(__name__)


class HTMLContentExtractor:
    """Service for extracting raw HTML content from article URLs"""
    
    def __init__(self, base_domain: Optional[str] = None, timeout: Optional[int] = None):
        self.base_domain = base_domain or Config.CRAWLER_BASE_DOMAIN
        self.timeout = timeout or Config.CRAWLER_REQUEST_TIMEOUT
        self.session = requests.Session()
        
        # Set user agent to avoid being blocked
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        logger.info(f" HTMLContentExtractor initialized - base domain: {self.base_domain}")
    
    def extract_html_content(self, article: Article) -> Optional[str]:
        """
        Extract raw HTML content from article URL
        
        Args:
            article: Article object containing the link
            
        Returns:
            Raw HTML content as string, or None if extraction fails
        """
        if not article.link:
            logger.warning(f"� No link provided for article: {article.title}")
            return None
        
        try:
            # Validate and normalize URL
            url = self._normalize_url(article.link)
            if not url:
                logger.warning(f"� Invalid URL: {article.link}")
                return None
            
            logger.debug(f"< Extracting HTML from: {url}")
            
            # Make request with timeout
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            # Check if content is HTML
            content_type = response.headers.get('content-type', '').lower()
            if 'text/html' not in content_type:
                logger.warning(f"� Non-HTML content type for {url}: {content_type}")
                return None
            
            # Return raw HTML content
            html_content = response.text
            logger.debug(f" Extracted {len(html_content)} characters from {url}")
            
            return html_content
            
        except requests.exceptions.Timeout:
            logger.error(f"� Timeout extracting HTML from {article.link}")
        except requests.exceptions.RequestException as e:
            logger.error(f"L Error extracting HTML from {article.link}: {e}")
        except Exception as e:
            logger.error(f"L Unexpected error extracting HTML from {article.link}: {e}")
        
        return None
    
    def extract_article_content(self, article: Article) -> Dict[str, Any]:
        """
        Extract both raw HTML and structured content from article
        
        Args:
            article: Article object containing the link
            
        Returns:
            Dictionary with extraction results
        """
        result = {
            'link': article.link,
            'extraction_success': False,
            'raw_html': None,
            'content_hash': None,
            'extracted_at': None,
            'error': None
        }
        
        try:
            # Extract raw HTML
            raw_html = self.extract_html_content(article)
            if not raw_html:
                result['error'] = 'Failed to extract HTML content'
                return result
            
            # Generate content hash for change detection
            content_hash = hashlib.md5(raw_html.encode('utf-8')).hexdigest()
            
            # Optional: Extract main content area (can be extended)
            main_content = self._extract_main_content(raw_html)
            
            result.update({
                'extraction_success': True,
                'raw_html': raw_html,
                'main_content': main_content,
                'content_hash': content_hash,
                'extracted_at': article.crawled_at
            })
            
            logger.debug(f" Successfully extracted content from {article.link}")
            
        except Exception as e:
            logger.error(f"L Error in extract_article_content for {article.link}: {e}")
            result['error'] = str(e)
        
        return result
    
    def _normalize_url(self, url: str) -> Optional[str]:
        """Normalize and validate URL"""
        if not url or not isinstance(url, str):
            return None
        
        url = url.strip()
        
        # If URL is relative, make it absolute
        if url.startswith('/'):
            return urljoin(self.base_domain, url)
        elif not url.startswith(('http://', 'https://')):
            return urljoin(self.base_domain, url)
        
        # Validate URL format
        try:
            parsed = urlparse(url)
            if not parsed.netloc:
                return None
            return url
        except Exception:
            return None
    
    def _extract_main_content(self, html: str) -> Optional[str]:
        """
        Extract main content from HTML (basic implementation)
        This can be extended with more sophisticated content extraction
        
        Args:
            html: Raw HTML content
            
        Returns:
            Main content as string, or None if extraction fails
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove script, style, and other non-content elements
            for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
                element.decompose()
            
            # Try to find main content areas (Vietstock specific)
            content_selectors = [
                '.article-content',
                '.content',
                '.main-content',
                '#main-content',
                '.post-content',
                '.entry-content',
                'article',
                '.news-content'
            ]
            
            main_content = None
            for selector in content_selectors:
                main_content = soup.select_one(selector)
                if main_content:
                    break
            
            if main_content:
                return main_content.get_text(strip=True)
            else:
                # Fallback to body content
                body = soup.find('body')
                if body:
                    return body.get_text(strip=True)
            
        except Exception as e:
            logger.debug(f"� Error extracting main content: {e}")
        
        return None
    
    def extract_batch(self, articles: list, delay: Optional[float] = None) -> Dict[str, Any]:
        """
        Extract HTML content for multiple articles with rate limiting
        
        Args:
            articles: List of Article objects
            delay: Delay between requests (defaults to config value)
            
        Returns:
            Dictionary with batch extraction results
        """
        delay = delay or Config.CRAWLER_RATE_LIMIT_DELAY
        
        results = {
            'total_articles': len(articles),
            'successful_extractions': 0,
            'failed_extractions': 0,
            'results': [],
            'extraction_time': None
        }
        
        start_time = time.time()
        
        for i, article in enumerate(articles):
            logger.debug(f"[{i+1}/{len(articles)}] Extracting: {article.title}")
            
            extraction_result = self.extract_article_content(article)
            results['results'].append(extraction_result)
            
            if extraction_result['extraction_success']:
                results['successful_extractions'] += 1
            else:
                results['failed_extractions'] += 1
                logger.warning(f"� Failed to extract: {article.link} - {extraction_result.get('error', 'Unknown error')}")
            
            # Rate limiting
            if i < len(articles) - 1:  # Don't delay after last article
                time.sleep(delay)
        
        results['extraction_time'] = time.time() - start_time
        
        logger.info(f"=� Batch extraction completed: {results['successful_extractions']}/{results['total_articles']} successful")
        logger.info(f"� Total extraction time: {results['extraction_time']:.2f}s")
        
        return results
    
    def close(self):
        """Close session and cleanup resources"""
        if self.session:
            self.session.close()
            logger.info("= HTML extractor session closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Factory function for easy instantiation
def create_html_extractor(base_domain: Optional[str] = None, timeout: Optional[int] = None) -> HTMLContentExtractor:
    """Create HTML extractor instance with default configuration"""
    return HTMLContentExtractor(base_domain, timeout)