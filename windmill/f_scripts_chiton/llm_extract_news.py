import wmill
from openai import OpenAI
from pymongo import MongoClient
from datetime import datetime
import json
from typing import Dict, Optional
import uuid

# ================== CONFIG OPENROUTER ==================
def _normalize_base_url(url: Optional[str]) -> str:
    """Đảm bảo URL có dạng .../api/v1"""
    url = (url or "").strip().rstrip("/")
    if not url:
        raise ValueError("Missing OpenRouter base URL variable: f/variables_chiton/open_router_url")
    if not url.endswith("/api/v1"):
        if url.endswith("/api"):
            url = url + "/v1"
        elif url.endswith("/v1"):
            if not url.endswith("/api/v1"):
                url = url.replace("/v1", "/api/v1")
        else:
            url = url + "/api/v1"
    return url

# Lấy biến Windmill
_BASE_URL = _normalize_base_url(wmill.get_variable("f/variables_chiton/open_router_url"))
_API_KEY = (wmill.get_variable("f/variables_chiton/open_ai_key") or "").strip()
if not _API_KEY:
    raise ValueError("Missing OpenRouter API key variable: f/variables_chiton/open_ai_key")

# Khởi tạo client
client = OpenAI(base_url=_BASE_URL, api_key=_API_KEY)  # ví dụ: https://openrouter.ai/api/v1

# ================== JSON SCHEMA (ESCAPED CHO .format) ==================
SCHEMA = r'''{
  "content": {
    "headline": "Main headline",
    "subheadline": "Subheadline if exists",
    "summary": "Brief 2-3 sentence summary",
    "body": "Full article text, cleaned",
    "author": "Author name if available"
  },
  "source": {
    "name": "Source name (Bloomberg, Reuters, etc.)",
    "credibility_score": 0.0
  },
  "timing": {
    "published_at": "ISO datetime",
    "market_session": "pre_market|market_hours|after_hours|closed"
  },
  "classification": {
    "primary_category": "earnings|m&a|guidance|product_launch|regulatory|management_change|economic_data|other",
    "sub_categories": ["list", "of", "subcategories"],
    "topics": ["list", "of", "topics"]
  },
  "companies_mentioned": [
    {
      "ticker": "AAPL",
      "company_name": "Apple Inc.",
      "relevance_score": 0.0,
      "mention_type": "primary_subject|secondary_subject|mentioned|compared_to",
      "sentiment": 0.0,
      "context": "Brief context of how company is mentioned"
    }
  ],
  "events_extracted": [
    {
      "event_type": "earnings_beat|earnings_miss|dividend_increase|stock_split|merger_announced|ceo_change|product_launch|guidance_raise|guidance_lower|other",
      "description": "Brief description",
      "companies_affected": ["AAPL", "MSFT"],
      "impact_magnitude": 0.0,
      "confidence": 0.0
    }
  ],
  "sentiment": {
    "overall_sentiment": 0.0,
    "sentiment_magnitude": 0.0,
    "emotional_tone": {
      "fear": 0.0,
      "greed": 0.0,
      "optimism": 0.0,
      "pessimism": 0.0,
      "urgency": 0.0
    },
    "market_impact_score": 0.0
  },
  "confidence_score": 0.0
}'''
SCHEMA_ESCAPED = SCHEMA.replace('{', '{{').replace('}', '}}')

# ================== PROMPT (chèn {schema}) ==================
EXTRACTION_PROMPT = """
You are a financial news extraction expert. Extract structured information from the following news article.

**ARTICLE METADATA:**
- Title: {article_title}
- Publication Date: {publication_date}

**ARTICLE TEXT:**
{article_text}

**EXTRACTION INSTRUCTIONS:**

Extract and return a JSON object with the following structure:

{schema}

GUIDELINES:
- If information is missing, use null or an empty string.
- Always return valid JSON only (no markdown, no extra text).
"""

# ================== PARSE JSON AN TOÀN ==================
def _safe_json_loads(s: Optional[str]) -> Dict:
    raw = (s or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # gỡ ```json ... ``` nếu có
        if raw.startswith("```"):
            raw_stripped = raw.strip("`").strip()
            try:
                return json.loads(raw_stripped)
            except Exception:
                pass
        return {}

# ================== LLM EXTRACTOR ==================
def extract_news_with_llm(
    html_text: str,
    metadata: Dict,
    model: str = "openai/gpt-oss-20b:free",  # model mặc định (OpenRouter)
) -> Dict:
    # ✅ KHÔI PHỤC GIỚI HẠN an toàn để tránh quá context
    prompt = EXTRACTION_PROMPT.format(
        article_text=(html_text or "")[:50000],
        article_title=(metadata or {}).get("title", "") or "",
        publication_date=(metadata or {}).get("publication_date", "") or "",
        schema=SCHEMA_ESCAPED,
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a financial news extraction expert. Output JSON only."},
            {"role": "user", "content": prompt},
        ],
        extra_headers={
            "HTTP-Referer": "https://windmill.pythera.ai",
            "X-Title": "Extract with LLM By ChiTon",
        },
        temperature=0.1,
        max_tokens=4096,
        response_format={"type": "json_object"},
    )

    return {
        "extracted": _safe_json_loads(response.choices[0].message.content),
        "usage": {
            "prompt_tokens": getattr(response.usage, "prompt_tokens", 0),
            "completion_tokens": getattr(response.usage, "completion_tokens", 0),
            "total_tokens": getattr(response.usage, "total_tokens", 0),
        },
        "model": model,
    }

# ================== MAIN FUNCTION ==================
def main(document_id: str, max_retries: int = 3) -> dict:
    mongo_client = MongoClient(wmill.get_variable("u/oudev2/mongo_uri_chiton"))
    db = mongo_client.financial_news

    raw_doc = db.raw_documents.find_one({"_id": document_id})
    if not raw_doc:
        return {"success": False, "error": "Document not found"}

    db.raw_documents.update_one(
        {"_id": document_id},
        {"$set": {"processing_status.status": "processing"}},
    )

    try:
        # Trích xuất với LLM
        result = extract_news_with_llm(
            html_text=((raw_doc.get("content") or {}).get("text") or ""),
            metadata=raw_doc.get("metadata", {}) or {},
        )
        extracted = result["extracted"] or {}

        # ✅ VALIDATE: nếu JSON rỗng/thiếu headline → coi như lỗi để retry
        min_ok = bool(extracted.get("content")) and bool(extracted.get("content", {}).get("headline"))
        if not min_ok:
            raise ValueError("Empty extraction result (missing content.headline)")

        # Ghi vào news_articles
        article_doc = {
            "_id": str(uuid.uuid4()),
            "raw_document_id": document_id,
            "content": extracted.get("content", {}),
            "source": extracted.get("source", {}),
            "timing": extracted.get("timing", {}),
            "classification": extracted.get("classification", {}),
            "companies_mentioned": extracted.get("companies_mentioned", []),
            "events_extracted": extracted.get("events_extracted", []),
            "sentiment": extracted.get("sentiment", {}),
            "extraction": {
                "model": result["model"],
                "extracted_at": datetime.utcnow(),
                "confidence_score": extracted.get("confidence_score", 0.0),
                "token_usage": result["usage"],
            },
            "created_at": datetime.utcnow(),
        }

        db.news_articles.insert_one(article_doc)

        db.raw_documents.update_one(
            {"_id": document_id},
            {
                "$set": {
                    "processing_status.status": "completed",
                    "processing_status.processed_at": datetime.utcnow(),
                }
            },
        )

        return {
            "success": True,
            "article_id": article_doc["_id"],
            "document_id": document_id,
            "companies_found": len(article_doc["companies_mentioned"]),
            "sentiment": article_doc["sentiment"].get("overall_sentiment"),
            "token_usage": result["usage"],
        }

    except Exception as e:
        retry_count = (raw_doc.get("processing_status") or {}).get("retry_count", 0)

        if retry_count < max_retries:
            db.raw_documents.update_one(
                {"_id": document_id},
                {
                    "$set": {
                        "processing_status.status": "pending",
                        "processing_status.retry_count": retry_count + 1,
                        "processing_status.last_error": str(e),
                    }
                },
            )
        else:
            db.raw_documents.update_one(
                {"_id": document_id},
                {
                    "$set": {
                        "processing_status.status": "failed",
                        "processing_status.error_message": str(e),
                        "processing_status.last_error": str(e),
                    }
                },
            )

        return {"success": False, "error": str(e), "retry_count": retry_count}
