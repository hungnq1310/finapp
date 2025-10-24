import hashlib
import wmill
import feedparser
from datetime import datetime, timedelta
from pymongo import MongoClient
from typing import List, Dict, Optional

# --- Parse time từ entry ---
def _parse_pub(entry) -> Optional[datetime]:
    t = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if not t:
        return None
    return datetime(*t[:6])  # naive UTC từ feedparser

# --- Playwright fetch (vượt chặn) ---
# windmill: {"tags": ["chromium"]}
from playwright.sync_api import sync_playwright

def browser_fetch_and_parse(url: str):
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"])
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        ))
        page = context.new_page()
        resp = page.goto(url, wait_until="domcontentloaded", timeout=60000)
        if not resp or not (200 <= resp.status <= 299):
            raise RuntimeError(f"HTTP {resp.status if resp else 'no response'} for {url}")
        content = resp.body()  # bytes XML
        browser.close()
    return feedparser.parse(content)

def safe_parse_feed(url: str):
    """Ưu tiên Playwright; lỗi thì fallback về feedparser.parse(url)."""
    try:
        return browser_fetch_and_parse(url)
    except Exception as e:
        print(f"[Playwright] fallback for {url}: {e}")
        return feedparser.parse(url)

# --- Core ---
def check_rss_feeds(feeds: List[str], lookback_hours: int = 48) -> List[Dict]:
    client = MongoClient(wmill.get_variable("u/oudev2/mongo_uri_chiton"))
    db = client.financial_news

    new_articles: List[Dict] = []
    cutoff_time = datetime.utcnow() - timedelta(hours=lookback_hours)

    for feed_url in feeds:
        try:
            feed = safe_parse_feed(feed_url)  # <-- dùng Playwright + fallback

            for entry in feed.entries:
                pub_date = _parse_pub(entry)
                if not pub_date or pub_date < cutoff_time:
                    continue

                link = entry.get("link", "")
                if not link:
                    continue

                content_hash = hashlib.sha256(link.encode("utf-8")).hexdigest()

                # dedupe theo Mongo
                existing = db.raw_documents.find_one({"metadata.content_hash": content_hash})
                if existing:
                    continue

                new_articles.append({
                    "url": link,
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", ""),
                    "published_at": pub_date,
                    "source_feed": feed_url,
                    "content_hash": content_hash,
                })

        except Exception as e:
            print(f"Error parsing feed {feed_url}: {e}")
            continue

    return new_articles

# --- Windmill entrypoint ---
def main(feeds: list = None) -> dict:
    if feeds is None:
        # LUÔN là list URL, không gọi browser_fetch_and_parse ở đây
        feeds = ["https://www.marketwatch.com/rss/topstories"]

    articles = check_rss_feeds(feeds)

    return {
        "new_articles": articles,
        "count": len(articles),
        "timestamp": datetime.utcnow().isoformat(),
    }
