import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
import uuid
from typing import Dict
import wmill
from f.scripts_giaphuc.error_handler_common import main as log_error


def fetch_html_content(url: str, timeout: int = 30) -> Dict:
    """
    Fetch HTML content from URL and extract clean text
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; FinancialNewsBot/1.0)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()

    # Parse HTML
    soup = BeautifulSoup(response.content, "html.parser")

    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()

    # Extract text
    text = soup.get_text(separator="\\n", strip=True)

    return {
        "html": response.text,
        "text": text,
        "encoding": response.encoding,
        "size_bytes": len(response.content),
        "http_status": response.status_code,
        "response_time_ms": int(response.elapsed.total_seconds() * 1000),
    }


def main(article: dict) -> dict:
    """
    Windmill step: Fetch and store raw HTML
    """
    client = MongoClient(wmill.get_variable("u/oudev2/mongo_uri_chiton"))
    db = client.financial_news

    doc_id = None

    try:
        content_data = fetch_html_content(article["url"])

        # Create raw document
        doc_id = str(uuid.uuid4())
        raw_doc = {
            "_id": doc_id,
            "source": {
                "url": article["url"],
                "domain": extract_domain(article["url"]),
                "source_type": "news",
            },
            "content": {
                "html": content_data["html"],
                "text": content_data["text"],
                "file_type": "html",
                "encoding": content_data["encoding"],
                "size_bytes": content_data["size_bytes"],
            },
            "metadata": {
                "title": article.get("title", ""),
                "publication_date": article.get("published_at"),
                "content_hash": article.get("content_hash"),
            },
            "crawl_info": {
                "crawled_at": datetime.utcnow(),
                "crawler_version": "v1.0.0",
                "http_status": content_data["http_status"],
                "response_time_ms": content_data["response_time_ms"],
            },
            "processing_status": {"status": "pending", "retry_count": 0},
        }

        db.raw_documents.insert_one(raw_doc)

        return {
            "success": True,
            "document_id": doc_id,
            "url": article["url"],
            "size_bytes": content_data["size_bytes"],
        }

    except Exception as e:
        error_params = {
            "owner": "ton",
            "document_id": doc_id or article.get("url", "unknown"),  
            "error_message": str(e), 
            "retry_count": 0,  
            "context": {  
                "url": article.get("url") if article else None,
                "title": article.get("title") if article else None,
                "error_type": type(e).__name__  
            }
        }

        try:
            log_result = log_error(error_params) 
            print(f"Error logged to Phúc's monitoring: {log_result}")
        except Exception as log_ex:
            print(f"Error logging failed: {log_ex}")

        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,  
            "url": article.get("url") if article else None
        }


def extract_domain(url: str) -> str:
    from urllib.parse import urlparse

    return urlparse(url).netloc
