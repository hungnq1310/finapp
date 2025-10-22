import wmill
from openai import OpenAI  # THAY ĐỔI: Import OpenAI class
from pymongo import MongoClient
from datetime import datetime
import json
from typing import Dict, Optional
import uuid


client = OpenAI(
    base_url=wmill.get_variable("f/variables_theanh/open_router_url"),
    api_key=wmill.get_variable("f/variables_theanh/open_ai_key"),
)
# Xem lại nội dung prompt
EXTRACTION_PROMPT = """
You are a financial news extraction expert. Extract structured information from the following news article.

**ARTICLE METADATA:**
- Title: {article_title}
- Publication Date: {publication_date}

**ARTICLE TEXT:**
{article_text}

**EXTRACTION INSTRUCTIONS:**

Extract and return a JSON object with the following structure:

{{
  "content": {{
    "headline": "Main headline",
    "subheadline": "Subheadline if exists",
    "summary": "Brief 2-3 sentence summary",
    "body": "Full article text, cleaned",
    "author": "Author name if available"
  }},
  
  "source": {{
    "name": "Source name (Bloomberg, Reuters, etc.)",
    "credibility_score": 0.0-1.0
  }},
  
  "timing": {{
    "published_at": "ISO datetime",
    "market_session": "pre_market|market_hours|after_hours|closed"
  }},
  
  "classification": {{
    "primary_category": "earnings|m&a|guidance|product_launch|regulatory|management_change|economic_data|other",
    "sub_categories": ["list", "of", "subcategories"],
    "topics": ["list", "of", "topics"]
  }},
  
  "companies_mentioned": [
    {{
      "ticker": "AAPL",
      "company_name": "Apple Inc.",
      "relevance_score": 0.0-1.0,
      "mention_type": "primary_subject|secondary_subject|mentioned|compared_to",
      "sentiment": -1.0 to 1.0,
      "context": "Brief context of how company is mentioned"
    }}
  ],
  
  "events_extracted": [
    {{
      "event_type": "earnings_beat|earnings_miss|dividend_increase|stock_split|merger_announced|ceo_change|product_launch|guidance_raise|guidance_lower|other",
      "description": "Brief description",
      "companies_affected": ["AAPL", "MSFT"],
      "impact_magnitude": numeric value if applicable (e.g., % beat),
      "confidence": 0.0-1.0
    }}
  ],
  
  "sentiment": {{
    "overall_sentiment": -1.0 to 1.0,
    "sentiment_magnitude": 0.0-1.0,
    "emotional_tone": {{
      "fear": 0.0-1.0,
      "greed": 0.0-1.0,
      "optimism": 0.0-1.0,
      "pessimism": 0.0-1.0,
      "urgency": 0.0-1.0
    }},
    "market_impact_score": 0.0-1.0
  }},
  
  "confidence_score": 0.0-1.0
}}

**GUIDELINES:**

1. **Companies**: Identify ALL companies mentioned. Use standard tickers (US exchanges).
   - Primary subject: The main company the article is about
   - Secondary subject: Other important companies discussed
   - Mentioned: Companies briefly referenced
   - Compared to: Companies used for comparison

2. **Sentiment**: 
   - -1.0 = Very negative (bankruptcy, scandal, massive losses)
   - -0.5 = Moderately negative (earnings miss, downgrade)
   - 0.0 = Neutral (factual reporting)
   - +0.5 = Moderately positive (earnings beat, upgrade)
   - +1.0 = Very positive (breakthrough, major success)

3. **Events**: Extract specific, actionable events. Include numerical data when available.

4. **Categories**: 
   - Primary: The main topic
   - Sub-categories: Specific aspects
   - Topics: Broader themes

5. **Confidence**: Your confidence in the extraction accuracy (0.0-1.0)

6. **Market Session** (US Eastern Time):
   - pre_market: Before 9:30 AM ET
   - market_hours: 9:30 AM - 4:00 PM ET
   - after_hours: 4:00 PM - 8:00 PM ET
   - closed: All other times

Return ONLY the JSON object, no additional text.
"""


def extract_news_with_llm(
    html_text: str, metadata: Dict, model: str = "openai/gpt-oss-20b:free"
) -> Dict:
    prompt = EXTRACTION_PROMPT.format(
        article_text=html_text[:50000],  # Token limit
        article_title=metadata.get("title", ""),
        publication_date=metadata.get("publication_date", ""),
    )

    # THAY ĐỔI: Sử dụng syntax mới cho OpenAI v1.0.0+
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": "You are a financial news extraction expert. Extract structured information from articles.",
            },
            {"role": "user", "content": prompt},
        ],
        extra_headers={  # THAY ĐỔI: Dùng extra_headers
            "HTTP-Referer": "https://windmill.pythera.ai",
            "X-Title": "Extract with LLM By TheAnh",
        },
        temperature=0.1,
        max_tokens=4096,
        response_format={"type": "json_object"},
    )

    # THAY ĐỔI: Syntax truy cập response mới
    extracted_data = json.loads(response.choices[0].message.content)
    return {
        "extracted": extracted_data,
        "usage": {
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens,
        },
        "model": model,
    }


def main(document_id: str, max_retries: int = 3) -> dict:
    # max_retries: int = 3 là số lần retry lại hàm này
    client = MongoClient(wmill.get_variable("u/oudev2/mongo_uri_theanh"))
    db = client.financial_news

    # Lấy raw trên mongodb
    raw_doc = db.raw_documents.find_one({"_id": document_id})
    if not raw_doc:
        return {"success": False, "error": "Document not found"}

    # CẬP NHẬT status to processing
    db.raw_documents.update_one(
        {"_id": document_id}, {"$set": {"processing_status.status": "processing"}}
    )

    try:
        # Trích xuất with LLM
        result = extract_news_with_llm(
            html_text=raw_doc["content"]["text"], metadata=raw_doc["metadata"]
        )

        extracted = result["extracted"]

        # Tạo news_articles document
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

        # Thêm vào news_articles
        db.news_articles.insert_one(article_doc)

        # Update lại raw document status
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
            "companies_found": len(extracted.get("companies_mentioned", [])),
            "sentiment": extracted.get("sentiment", {}).get("overall_sentiment"),
            "token_usage": result["usage"],
        }

    except Exception as e:
        # Handle error
        retry_count = raw_doc["processing_status"].get("retry_count", 0)

        if retry_count < max_retries:
            db.raw_documents.update_one(
                {"_id": document_id},
                {
                    "$set": {
                        "processing_status.status": "pending",
                        "processing_status.retry_count": retry_count + 1,
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
                    }
                },
            )

        return {"success": False, "error": str(e), "retry_count": retry_count}
